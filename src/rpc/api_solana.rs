use {
    crate::{
        config::{ConfigRpc, ConfigRpcCallJson},
        metrics::{
            RPC_REQUESTS_DURATION_SECONDS, RPC_REQUESTS_GENERATED_BYTES_TOTAL, RPC_REQUESTS_TOTAL,
            RPC_WORKERS_CPU_SECONDS_TOTAL, duration_to_seconds,
        },
        rpc::{
            api::{
                RpcResponse, check_call_support, get_x_bigtable_disabled, get_x_subscription_id,
                response_400, response_500,
            },
            upstream::RpcClientJsonrpc,
            workers::WorkRequest,
        },
        storage::{
            read::{
                ReadRequest, ReadResultBlock, ReadResultBlockHeight, ReadResultBlockTime,
                ReadResultBlockhashValid, ReadResultBlocks, ReadResultLatestBlockhash,
                ReadResultRecentPrioritizationFees, ReadResultSignatureStatuses,
                ReadResultSignaturesForAddress, ReadResultTransaction,
            },
            slots::StoredSlots,
        },
    },
    anyhow::Context,
    crossbeam::channel::{Sender, TrySendError},
    futures::stream::{FuturesOrdered, StreamExt},
    http_body_util::{BodyExt, Full as BodyFull, Limited},
    hyper::{
        body::{Bytes, Incoming as BodyIncoming},
        header::CONTENT_TYPE,
        http::Result as HttpResult,
    },
    jsonrpsee_types::{
        Id, Params, Request, Response, ResponsePayload, TwoPointZero,
        error::{ErrorCode, ErrorObject, ErrorObjectOwned, INVALID_PARAMS_MSG},
    },
    metrics::{counter, gauge, histogram},
    prost::Message,
    serde::{Deserialize, Serialize, de},
    solana_rpc_client_api::{
        config::{
            RpcBlockConfig, RpcBlocksConfigWrapper, RpcContextConfig, RpcEncodingConfigWrapper,
            RpcSignatureStatusConfig, RpcSignaturesForAddressConfig, RpcTransactionConfig,
        },
        custom_error::RpcCustomError,
        request::{MAX_GET_CONFIRMED_BLOCKS_RANGE, MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS},
        response::{
            RpcBlockhash, RpcConfirmedTransactionStatusWithSignature, RpcResponseContext,
            RpcVersionInfo,
        },
    },
    solana_sdk::{
        clock::{Slot, UnixTimestamp},
        commitment_config::{CommitmentConfig, CommitmentLevel},
        hash::Hash,
        pubkey::Pubkey,
        signature::Signature,
        transaction::MAX_TX_ACCOUNT_LOCKS,
    },
    solana_storage_proto::convert::generated,
    solana_transaction_status::{
        BlockEncodingOptions, ConfirmedBlock, ConfirmedTransactionWithStatusMeta,
        TransactionStatus, TransactionWithStatusMeta, UiTransactionEncoding,
    },
    std::{
        str::FromStr,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        sync::{mpsc, oneshot},
        time::sleep,
    },
    tracing::error,
};

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RpcRecentPrioritizationFeesConfig {
    pub percentile: Option<u16>,
}

type RpcRequestResult = anyhow::Result<Response<'static, serde_json::Value>>;

fn response_200<D: Into<Bytes>>(data: D) -> HttpResult<RpcResponse> {
    hyper::Response::builder()
        .header(CONTENT_TYPE, "application/json; charset=utf-8")
        .body(BodyFull::from(data.into()).boxed())
}

fn jsonrpc_response_success(
    id: Id<'_>,
    payload: serde_json::Value,
) -> Response<'_, serde_json::Value> {
    Response {
        jsonrpc: Some(TwoPointZero),
        payload: ResponsePayload::success(payload),
        id,
    }
}

fn jsonrpc_response_error(id: Id<'_>, error: ErrorObjectOwned) -> Response<'_, serde_json::Value> {
    Response {
        jsonrpc: Some(TwoPointZero),
        payload: ResponsePayload::error(error),
        id,
    }
}

fn jsonrpc_response_error_custom(
    id: Id<'_>,
    error: RpcCustomError,
) -> Response<'_, serde_json::Value> {
    let error = jsonrpc_core::Error::from(error);
    jsonrpc_response_error(
        id,
        ErrorObject::owned(error.code.code() as i32, error.message, error.data),
    )
}

fn jsonrpc_error_invalid_params<S: Serialize>(
    message: impl Into<String>,
    data: Option<S>,
) -> ErrorObjectOwned {
    ErrorObject::owned(ErrorCode::InvalidParams.code(), message, data)
}

#[derive(Debug, Clone, Copy)]
struct SupportedCalls {
    get_block: bool,
    get_block_height: bool,
    get_blocks: bool,
    get_blocks_with_limit: bool,
    get_block_time: bool,
    get_latest_blockhash: bool,
    get_recent_prioritization_fees: bool,
    get_signatures_for_address: bool,
    get_signature_statuses: bool,
    get_slot: bool,
    get_transaction: bool,
    get_version: bool,
    is_blockhash_valid: bool,
}

impl SupportedCalls {
    fn new(calls: &[ConfigRpcCallJson]) -> anyhow::Result<Self> {
        Ok(Self {
            get_block: check_call_support(calls, ConfigRpcCallJson::GetBlock)?,
            get_block_height: check_call_support(calls, ConfigRpcCallJson::GetBlockHeight)?,
            get_blocks: check_call_support(calls, ConfigRpcCallJson::GetBlocks)?,
            get_blocks_with_limit: check_call_support(
                calls,
                ConfigRpcCallJson::GetBlocksWithLimit,
            )?,
            get_block_time: check_call_support(calls, ConfigRpcCallJson::GetBlockTime)?,
            get_latest_blockhash: check_call_support(calls, ConfigRpcCallJson::GetLatestBlockhash)?,
            get_recent_prioritization_fees: check_call_support(
                calls,
                ConfigRpcCallJson::GetRecentPrioritizationFees,
            )?,
            get_signatures_for_address: check_call_support(
                calls,
                ConfigRpcCallJson::GetSignaturesForAddress,
            )?,
            get_signature_statuses: check_call_support(
                calls,
                ConfigRpcCallJson::GetSignatureStatuses,
            )?,
            get_slot: check_call_support(calls, ConfigRpcCallJson::GetSlot)?,
            get_transaction: check_call_support(calls, ConfigRpcCallJson::GetTransaction)?,
            get_version: check_call_support(calls, ConfigRpcCallJson::GetVersion)?,
            is_blockhash_valid: check_call_support(calls, ConfigRpcCallJson::IsBlockhashValid)?,
        })
    }

    fn get_method(&self, method: &str) -> RpcRequestMethod {
        match method {
            "getBlock" if self.get_block => RpcRequestMethod::GetBlock,
            "getBlockHeight" if self.get_block_height => RpcRequestMethod::GetBlockHeight,
            "getBlocks" if self.get_blocks => RpcRequestMethod::GetBlocks,
            "getBlocksWithLimit" if self.get_blocks_with_limit => {
                RpcRequestMethod::GetBlocksWithLimit
            }
            "getBlockTime" if self.get_block_time => RpcRequestMethod::GetBlockTime,
            "getLatestBlockhash" if self.get_latest_blockhash => {
                RpcRequestMethod::GetLatestBlockhash
            }
            "getRecentPrioritizationFees" if self.get_recent_prioritization_fees => {
                RpcRequestMethod::GetRecentPrioritizationFees
            }
            "getSignaturesForAddress" if self.get_signatures_for_address => {
                RpcRequestMethod::GetSignaturesForAddress
            }
            "getSignatureStatuses" if self.get_signature_statuses => {
                RpcRequestMethod::GetSignatureStatuses
            }
            "getSlot" if self.get_slot => RpcRequestMethod::GetSlot,
            "getTransaction" if self.get_transaction => RpcRequestMethod::GetTransaction,
            "getVersion" if self.get_version => RpcRequestMethod::GetVersion,
            "isBlockhashValid" if self.is_blockhash_valid => RpcRequestMethod::IsBlockhashValid,
            _ => RpcRequestMethod::Unknown,
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum RpcRequestMethod {
    GetBlock,
    GetBlockHeight,
    GetBlocks,
    GetBlocksWithLimit,
    GetBlockTime,
    GetLatestBlockhash,
    GetRecentPrioritizationFees,
    GetSignaturesForAddress,
    GetSignatureStatuses,
    GetSlot,
    GetTransaction,
    GetVersion,
    IsBlockhashValid,
    Unknown,
}

impl RpcRequestMethod {
    const fn as_str(self) -> &'static str {
        match self {
            Self::GetBlock => "getBlock",
            Self::GetBlockHeight => "getBlockHeight",
            Self::GetBlocks => "getBlocks",
            Self::GetBlocksWithLimit => "getBlocksWithLimit",
            Self::GetBlockTime => "getBlockTime",
            Self::GetLatestBlockhash => "getLatestBlockhash",
            Self::GetRecentPrioritizationFees => "getRecentPrioritizationFees",
            Self::GetSignaturesForAddress => "getSignaturesForAddress",
            Self::GetSignatureStatuses => "getSignatureStatuses",
            Self::GetSlot => "getSlot",
            Self::GetTransaction => "getTransaction",
            Self::GetVersion => "getVersion",
            Self::IsBlockhashValid => "isBlockhashValid",
            Self::Unknown => "unknown",
        }
    }
}

#[derive(Debug)]
pub struct State {
    stored_slots: StoredSlots,
    body_limit: usize,
    request_timeout: Duration,
    supported_calls: SupportedCalls,
    gsfa_limit: usize,
    gss_transaction_history: bool,
    grpf_percentile: bool,
    requests_tx: mpsc::Sender<ReadRequest>,
    upstream: Option<RpcClientJsonrpc>,
    workers: Sender<WorkRequest>,
}

impl State {
    pub fn new(
        config: ConfigRpc,
        stored_slots: StoredSlots,
        requests_tx: mpsc::Sender<ReadRequest>,
        workers: Sender<WorkRequest>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            stored_slots,
            body_limit: config.body_limit,
            request_timeout: config.request_timeout,
            supported_calls: SupportedCalls::new(&config.calls_jsonrpc)?,
            gsfa_limit: config.gsfa_limit,
            gss_transaction_history: config.gss_transaction_history,
            grpf_percentile: config.grpf_percentile,
            requests_tx,
            upstream: config
                .upstream_jsonrpc
                .map(RpcClientJsonrpc::new)
                .transpose()?,
            workers,
        })
    }
}

pub async fn on_request(
    req: hyper::Request<BodyIncoming>,
    state: Arc<State>,
) -> HttpResult<RpcResponse> {
    let (parts, body) = req.into_parts();

    let x_subscription_id: Arc<str> = get_x_subscription_id(&parts.headers);
    let upstream_disabled = get_x_bigtable_disabled(&parts.headers);

    let bytes = match Limited::new(body, state.body_limit).collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => return response_400(format!("{error:?}\n"), None),
    };

    let requests = match RpcRequests::parse(&bytes) {
        Ok(requests) => requests,
        Err(error) => return response_400(format!("{error:?}\n"), None),
    };
    let mut buffer = match requests {
        RpcRequests::Single(request) => match RpcRequest::process(
            Arc::clone(&state),
            request,
            Arc::clone(&x_subscription_id),
            upstream_disabled,
        )
        .await
        {
            Ok(response) => serde_json::to_vec(&response).expect("json serialization never fail"),
            Err(error) => return response_500(error),
        },
        RpcRequests::Batch(requests) => {
            let mut futures = FuturesOrdered::new();
            for request in requests {
                let state = Arc::clone(&state);
                let x_subscription_id = Arc::clone(&x_subscription_id);
                futures.push_back(RpcRequest::process(
                    state,
                    request,
                    x_subscription_id,
                    upstream_disabled,
                ));
            }

            let mut buffer = Vec::new();
            buffer.push(b'[');
            while let Some(result) = futures.next().await {
                match result {
                    Ok(response) => serde_json::to_writer(&mut buffer, &response)
                        .expect("json serialization never fail"),
                    Err(error) => return response_500(error),
                }
                if !futures.is_empty() {
                    buffer.push(b',');
                }
            }
            buffer.push(b']');
            buffer
        }
    };
    buffer.push(b'\n');
    counter!(
        RPC_REQUESTS_GENERATED_BYTES_TOTAL,
        "x_subscription_id" => x_subscription_id,
    )
    .increment(buffer.len() as u64);
    response_200(buffer)
}

#[derive(Debug)]
enum RpcRequests<'a> {
    Single(Request<'a>),
    Batch(Vec<Request<'a>>),
}

impl<'a> RpcRequests<'a> {
    fn parse(bytes: &'a Bytes) -> serde_json::Result<Self> {
        for i in 0..bytes.len() {
            if bytes[i] == b'[' {
                return serde_json::from_slice::<Vec<Request<'_>>>(bytes).map(Self::Batch);
            } else if bytes[i] == b'{' {
                break;
            }
        }
        serde_json::from_slice::<Request<'_>>(bytes).map(Self::Single)
    }
}

enum RpcRequest {
    Block(RpcRequestBlock),
    BlockHeight(RpcRequestBlockHeight),
    Blocks(RpcRequestBlocks),
    BlockTime(RpcRequestBlockTime),
    LatestBlockhash(RpcRequestLatestBlockhash),
    RecentPrioritizationFees(RpcRequestRecentPrioritizationFees),
    SignaturesForAddress(RpcRequestSignaturesForAddress),
    SignatureStatuses(RpcRequestsSignatureStatuses),
    Transaction(RpcRequestTransaction),
    IsBlockhashValid(RpcRequestIsBlockhashValid),
}

impl RpcRequest {
    async fn process(
        state: Arc<State>,
        request: Request<'_>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
    ) -> anyhow::Result<Response<'_, serde_json::Value>> {
        let ts = quanta::Instant::now();
        let method = state.supported_calls.get_method(request.method.as_ref());
        counter!(
            RPC_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => method.as_str(),
        )
        .increment(1);
        let result = match Self::parse(&state, request, Arc::clone(&x_subscription_id), method) {
            Ok(request) => match request {
                Self::Block(request) => request.process(state, upstream_disabled).await,
                Self::BlockHeight(request) => request.process(state).await,
                Self::Blocks(request) => request.process(state, upstream_disabled).await,
                Self::BlockTime(request) => request.process(state, upstream_disabled).await,
                Self::LatestBlockhash(request) => request.process(state).await,
                Self::RecentPrioritizationFees(request) => request.process(state).await,
                Self::SignaturesForAddress(request) => {
                    request.process(state, upstream_disabled).await
                }
                Self::SignatureStatuses(request) => request.process(state, upstream_disabled).await,
                Self::Transaction(request) => request.process(state, upstream_disabled).await,
                Self::IsBlockhashValid(request) => request.process(state).await,
            },
            Err(error) => Ok(error),
        };
        histogram!(
            RPC_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => x_subscription_id,
            "method" => method.as_str(),
        )
        .record(duration_to_seconds(ts.elapsed()));
        result
    }

    fn parse<'a>(
        state: &Arc<State>,
        request: Request<'a>,
        x_subscription_id: Arc<str>,
        method: RpcRequestMethod,
    ) -> Result<Self, Response<'a, serde_json::Value>> {
        match method {
            RpcRequestMethod::GetBlock => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    slot: Slot,
                    #[serde(default)]
                    config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
                }

                let (id, ReqParams { slot, config }) = Self::parse_params(request)?;

                let config = config
                    .map(|config| config.convert_to_current())
                    .unwrap_or_default();
                let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
                let encoding_options = BlockEncodingOptions {
                    transaction_details: config.transaction_details.unwrap_or_default(),
                    show_rewards: config.rewards.unwrap_or(true),
                    max_supported_transaction_version: config.max_supported_transaction_version,
                };
                let commitment = config.commitment.unwrap_or_default();
                if let Err(error) = Self::check_is_at_least_confirmed(commitment) {
                    return Err(jsonrpc_response_error(id, error));
                }

                Ok(Self::Block(RpcRequestBlock {
                    x_subscription_id,
                    id: id.into_owned(),
                    slot,
                    commitment,
                    encoding,
                    encoding_options,
                }))
            }
            RpcRequestMethod::GetBlockHeight => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    #[serde(default)]
                    config: Option<RpcContextConfig>,
                }

                let (id, ReqParams { config }) = Self::parse_params(request)?;
                let RpcContextConfig {
                    commitment,
                    min_context_slot,
                } = config.unwrap_or_default();
                let commitment = commitment.unwrap_or_default();

                let id = Self::min_context_check(id, min_context_slot, commitment, state)?;

                Ok(Self::BlockHeight(RpcRequestBlockHeight {
                    id: id.into_owned(),
                    commitment,
                }))
            }
            RpcRequestMethod::GetBlocks => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    start_slot: Slot,
                    #[serde(default)]
                    wrapper: Option<RpcBlocksConfigWrapper>,
                    #[serde(default)]
                    config: Option<RpcContextConfig>,
                }

                let (
                    id,
                    ReqParams {
                        start_slot,
                        wrapper,
                        config,
                    },
                ) = Self::parse_params(request)?;
                let (end_slot, maybe_config) =
                    wrapper.map(|wrapper| wrapper.unzip()).unwrap_or_default();
                let config = config.or(maybe_config).unwrap_or_default();

                let commitment = config.commitment.unwrap_or_default();
                if let Err(error) = Self::check_is_at_least_confirmed(commitment) {
                    return Err(jsonrpc_response_error(id, error));
                }

                let min_context_slot = config.min_context_slot.unwrap_or_default();
                let finalized_slot = state.stored_slots.finalized_load();
                if commitment.is_finalized() && finalized_slot < min_context_slot {
                    return Err(jsonrpc_response_error_custom(
                        id,
                        RpcCustomError::MinContextSlotNotReached {
                            context_slot: finalized_slot,
                        },
                    ));
                }

                let end_slot = std::cmp::min(
                    end_slot.unwrap_or_else(|| {
                        start_slot.saturating_add(MAX_GET_CONFIRMED_BLOCKS_RANGE)
                    }),
                    if commitment.is_finalized() {
                        finalized_slot
                    } else {
                        state.stored_slots.confirmed_load()
                    },
                );
                if end_slot < start_slot {
                    return Err(jsonrpc_response_success(id, serde_json::json!([])));
                }
                if end_slot - start_slot > MAX_GET_CONFIRMED_BLOCKS_RANGE {
                    return Err(jsonrpc_response_error(
                        id,
                        jsonrpc_error_invalid_params::<()>(
                            format!("Slot range too large; max {MAX_GET_CONFIRMED_BLOCKS_RANGE}"),
                            None,
                        ),
                    ));
                }

                Ok(Self::Blocks(RpcRequestBlocks {
                    x_subscription_id,
                    id: id.into_owned(),
                    start_slot,
                    until: RpcRequestBlocksUntil::EndSlot(end_slot),
                    commitment,
                }))
            }
            RpcRequestMethod::GetBlocksWithLimit => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    start_slot: Slot,
                    limit: usize,
                    #[serde(default)]
                    config: Option<RpcContextConfig>,
                }

                let (
                    id,
                    ReqParams {
                        start_slot,
                        limit,
                        config,
                    },
                ) = Self::parse_params(request)?;
                let config = config.unwrap_or_default();

                let commitment = config.commitment.unwrap_or_default();
                if let Err(error) = Self::check_is_at_least_confirmed(commitment) {
                    return Err(jsonrpc_response_error(id, error));
                }

                let min_context_slot = config.min_context_slot.unwrap_or_default();
                let finalized_slot = state.stored_slots.finalized_load();
                if commitment.is_finalized() && finalized_slot < min_context_slot {
                    return Err(jsonrpc_response_error_custom(
                        id,
                        RpcCustomError::MinContextSlotNotReached {
                            context_slot: finalized_slot,
                        },
                    ));
                }

                if limit == 0 {
                    return Err(jsonrpc_response_success(id, serde_json::json!([])));
                }
                if limit > MAX_GET_CONFIRMED_BLOCKS_RANGE as usize {
                    return Err(jsonrpc_response_error(
                        id,
                        jsonrpc_error_invalid_params::<()>(
                            format!("Limit too large; max {MAX_GET_CONFIRMED_BLOCKS_RANGE}"),
                            None,
                        ),
                    ));
                }

                Ok(Self::Blocks(RpcRequestBlocks {
                    x_subscription_id,
                    id: id.into_owned(),
                    start_slot,
                    until: RpcRequestBlocksUntil::Limit(limit),
                    commitment,
                }))
            }
            RpcRequestMethod::GetBlockTime => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    slot: Slot,
                }

                let (id, ReqParams { slot }) = Self::parse_params(request)?;

                if slot == 0 {
                    Err(jsonrpc_response_success(id, 1584368940.into()))
                } else {
                    Ok(Self::BlockTime(RpcRequestBlockTime {
                        x_subscription_id,
                        id: id.into_owned(),
                        slot,
                    }))
                }
            }
            RpcRequestMethod::GetLatestBlockhash => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    #[serde(default)]
                    config: Option<RpcContextConfig>,
                }

                let (id, ReqParams { config }) = Self::parse_params(request)?;
                let RpcContextConfig {
                    commitment,
                    min_context_slot,
                } = config.unwrap_or_default();
                let commitment = commitment.unwrap_or_default();

                let id = Self::min_context_check(id, min_context_slot, commitment, state)?;

                Ok(Self::LatestBlockhash(RpcRequestLatestBlockhash {
                    id: id.into_owned(),
                    commitment,
                }))
            }
            RpcRequestMethod::GetRecentPrioritizationFees => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    #[serde(default)]
                    pubkey_strs: Option<Vec<String>>,
                    #[serde(default)]
                    config: Option<RpcRecentPrioritizationFeesConfig>,
                }

                let (
                    id,
                    ReqParams {
                        pubkey_strs,
                        config,
                    },
                ) = Self::parse_params(request)?;

                let pubkey_strs = pubkey_strs.unwrap_or_default();
                if pubkey_strs.len() > MAX_TX_ACCOUNT_LOCKS {
                    return Err(jsonrpc_response_error(
                        id,
                        jsonrpc_error_invalid_params::<()>(
                            format!("Too many inputs provided; max {MAX_TX_ACCOUNT_LOCKS}"),
                            None,
                        ),
                    ));
                }
                let pubkeys = match pubkey_strs
                    .into_iter()
                    .map(|pubkey_str| Self::verify_pubkey(&pubkey_str))
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(pubkeys) => pubkeys,
                    Err(error) => {
                        return Err(jsonrpc_response_error(id, error));
                    }
                };

                let percentile = if state.grpf_percentile {
                    let RpcRecentPrioritizationFeesConfig { percentile } =
                        config.unwrap_or_default();
                    if let Some(percentile) = percentile {
                        if percentile > 10_000 {
                            return Err(jsonrpc_response_error(
                                id,
                                jsonrpc_error_invalid_params::<()>(
                                    "Percentile is too big; max value is 10000",
                                    None,
                                ),
                            ));
                        }
                    }
                    percentile
                } else {
                    None
                };

                Ok(Self::RecentPrioritizationFees(
                    RpcRequestRecentPrioritizationFees {
                        id: id.into_owned(),
                        pubkeys,
                        percentile,
                    },
                ))
            }
            RpcRequestMethod::GetSignaturesForAddress => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    address: String,
                    #[serde(default)]
                    config: Option<RpcSignaturesForAddressConfig>,
                }

                let (id, ReqParams { address, config }) = Self::parse_params(request)?;
                let RpcSignaturesForAddressConfig {
                    before,
                    until,
                    limit,
                    commitment,
                    min_context_slot,
                } = config.unwrap_or_default();

                let (address, before, until, limit) =
                    match Self::verify_and_parse_signatures_for_address_params(
                        address,
                        before,
                        until,
                        limit,
                        state.gsfa_limit,
                    ) {
                        Ok(value) => value,
                        Err(error) => return Err(jsonrpc_response_error(id, error)),
                    };

                let commitment = commitment.unwrap_or_default();
                if let Err(error) = Self::check_is_at_least_confirmed(commitment) {
                    return Err(jsonrpc_response_error(id, error));
                }

                let id = Self::min_context_check(id, min_context_slot, commitment, state)?;

                Ok(Self::SignaturesForAddress(RpcRequestSignaturesForAddress {
                    x_subscription_id,
                    id: id.into_owned(),
                    commitment,
                    address,
                    before,
                    until,
                    limit,
                }))
            }
            RpcRequestMethod::GetSignatureStatuses => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    signature_strs: Vec<String>,
                    config: Option<RpcSignatureStatusConfig>,
                }

                let (
                    id,
                    ReqParams {
                        signature_strs,
                        config,
                    },
                ) = Self::parse_params(request)?;

                if signature_strs.len() > MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS {
                    let message = format!(
                        "Too many inputs provided; max {MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS}"
                    );
                    return Err(jsonrpc_response_error(
                        id,
                        jsonrpc_error_invalid_params::<()>(message, None),
                    ));
                }

                let mut signatures: Vec<Signature> = Vec::with_capacity(signature_strs.len());
                for signature_str in signature_strs {
                    match Self::verify_signature(&signature_str) {
                        Ok(signature) => {
                            signatures.push(signature);
                        }
                        Err(error) => return Err(jsonrpc_response_error(id, error)),
                    }
                }

                let search_transaction_history = config
                    .map(|x| x.search_transaction_history)
                    .unwrap_or(false);

                if search_transaction_history && !state.gss_transaction_history {
                    return Err(jsonrpc_response_error_custom(
                        id,
                        RpcCustomError::TransactionHistoryNotAvailable,
                    ));
                }

                Ok(Self::SignatureStatuses(RpcRequestsSignatureStatuses {
                    x_subscription_id,
                    id: id.into_owned(),
                    signatures,
                    search_transaction_history,
                }))
            }
            RpcRequestMethod::GetSlot => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    #[serde(default)]
                    config: Option<RpcContextConfig>,
                }

                let (id, ReqParams { config }) = Self::parse_params(request)?;
                let RpcContextConfig {
                    commitment,
                    min_context_slot,
                } = config.unwrap_or_default();
                let commitment = commitment.unwrap_or_default();

                let context_slot = match commitment.commitment {
                    CommitmentLevel::Processed => state.stored_slots.processed_load(),
                    CommitmentLevel::Confirmed => state.stored_slots.confirmed_load(),
                    CommitmentLevel::Finalized => state.stored_slots.finalized_load(),
                };

                if let Some(min_context_slot) = min_context_slot {
                    if context_slot < min_context_slot {
                        return Err(jsonrpc_response_error_custom(
                            id,
                            RpcCustomError::MinContextSlotNotReached { context_slot },
                        ));
                    }
                }

                Err(jsonrpc_response_success(id, context_slot.into()))
            }
            RpcRequestMethod::GetTransaction => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    signature_str: String,
                    #[serde(default)]
                    config: Option<RpcEncodingConfigWrapper<RpcTransactionConfig>>,
                }

                let (
                    id,
                    ReqParams {
                        signature_str,
                        config,
                    },
                ) = Self::parse_params(request)?;

                let signature = match Self::verify_signature(&signature_str) {
                    Ok(signature) => signature,
                    Err(error) => return Err(jsonrpc_response_error(id, error)),
                };

                let config = config
                    .map(|config| config.convert_to_current())
                    .unwrap_or_default();
                let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
                let max_supported_transaction_version = config.max_supported_transaction_version;
                let commitment = config.commitment.unwrap_or_default();
                if let Err(error) = Self::check_is_at_least_confirmed(commitment) {
                    return Err(jsonrpc_response_error(id, error));
                }

                Ok(Self::Transaction(RpcRequestTransaction {
                    x_subscription_id,
                    id: id.into_owned(),
                    signature,
                    commitment,
                    encoding,
                    max_supported_transaction_version,
                }))
            }
            RpcRequestMethod::GetVersion => {
                if let Some(error) = match serde_json::from_str::<serde_json::Value>(
                    request.params.as_ref().map(|p| p.get()).unwrap_or("null"),
                ) {
                    Ok(value) => match value {
                        serde_json::Value::Null => None,
                        serde_json::Value::Array(vec) if vec.is_empty() => None,
                        value => Some(jsonrpc_error_invalid_params(
                            "No parameters were expected",
                            Some(value.to_string()),
                        )),
                    },
                    Err(error) => Some(jsonrpc_error_invalid_params(
                        INVALID_PARAMS_MSG,
                        Some(error.to_string()),
                    )),
                } {
                    Err(jsonrpc_response_error(request.id, error))
                } else {
                    let version = solana_version::Version::default();
                    Err(jsonrpc_response_success(
                        request.id,
                        serde_json::json!(RpcVersionInfo {
                            solana_core: version.to_string(),
                            feature_set: Some(version.feature_set),
                        }),
                    ))
                }
            }
            RpcRequestMethod::IsBlockhashValid => {
                #[derive(Debug, Deserialize)]
                struct ReqParams {
                    blockhash: String,
                    #[serde(default)]
                    config: Option<RpcContextConfig>,
                }

                let (id, ReqParams { blockhash, config }) = Self::parse_params(request)?;
                let RpcContextConfig {
                    commitment,
                    min_context_slot,
                } = config.unwrap_or_default();
                let commitment = commitment.unwrap_or_default();

                let id = Self::min_context_check(id, min_context_slot, commitment, state)?;

                if let Err(error) = Hash::from_str(&blockhash) {
                    return Err(jsonrpc_response_error(
                        id,
                        jsonrpc_error_invalid_params::<()>(format!("{error:?}"), None),
                    ));
                }

                Ok(Self::IsBlockhashValid(RpcRequestIsBlockhashValid {
                    id: id.into_owned(),
                    blockhash,
                    commitment,
                }))
            }
            RpcRequestMethod::Unknown => Err(Response {
                jsonrpc: Some(TwoPointZero),
                payload: ResponsePayload::error(ErrorCode::MethodNotFound),
                id: request.id,
            }),
        }
    }

    fn parse_params<'a, T>(
        request: Request<'a>,
    ) -> Result<(Id<'a>, T), Response<'a, serde_json::Value>>
    where
        T: for<'de> de::Deserialize<'de>,
    {
        let params = Params::new(request.params.as_ref().map(|p| p.get()));
        match params.parse() {
            Ok(params) => Ok((request.id, params)),
            Err(error) => Err(Response {
                jsonrpc: Some(TwoPointZero),
                payload: ResponsePayload::error(error),
                id: request.id,
            }),
        }
    }

    fn check_is_at_least_confirmed(commitment: CommitmentConfig) -> Result<(), ErrorObjectOwned> {
        if !commitment.is_at_least_confirmed() {
            return Err(jsonrpc_error_invalid_params::<()>(
                "Method does not support commitment below `confirmed`",
                None,
            ));
        }
        Ok(())
    }

    fn min_context_check<'a>(
        id: Id<'a>,
        min_context_slot: Option<Slot>,
        commitment: CommitmentConfig,
        state: &State,
    ) -> Result<Id<'a>, Response<'a, serde_json::Value>> {
        if let Some(min_context_slot) = min_context_slot {
            let context_slot = match commitment.commitment {
                CommitmentLevel::Processed => state.stored_slots.processed_load(),
                CommitmentLevel::Confirmed => state.stored_slots.confirmed_load(),
                CommitmentLevel::Finalized => state.stored_slots.finalized_load(),
            };

            if context_slot < min_context_slot {
                return Err(jsonrpc_response_error_custom(
                    id,
                    RpcCustomError::MinContextSlotNotReached { context_slot },
                ));
            }
        }
        Ok(id)
    }

    fn verify_signature(input: &str) -> Result<Signature, ErrorObjectOwned> {
        input.parse().map_err(|error| {
            jsonrpc_error_invalid_params::<()>(format!("Invalid param: {error:?}"), None)
        })
    }

    fn verify_pubkey(input: &str) -> Result<Pubkey, ErrorObjectOwned> {
        input.parse().map_err(|error| {
            jsonrpc_error_invalid_params::<()>(format!("Invalid param: {error:?}"), None)
        })
    }

    fn verify_and_parse_signatures_for_address_params(
        address: String,
        before: Option<String>,
        until: Option<String>,
        limit: Option<usize>,
        default_limit: usize, // default is MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT / 1000
    ) -> Result<(Pubkey, Option<Signature>, Option<Signature>, usize), ErrorObjectOwned> {
        let address = Self::verify_pubkey(&address)?;
        let before = before
            .map(|ref before| Self::verify_signature(before))
            .transpose()?;
        let until = until
            .map(|ref until| Self::verify_signature(until))
            .transpose()?;
        let limit = limit.unwrap_or(default_limit);

        if limit == 0 || limit > default_limit {
            Err(jsonrpc_error_invalid_params::<()>(
                format!("Invalid limit; max {default_limit}"),
                None,
            ))
        } else {
            Ok((address, before, until, limit))
        }
    }

    async fn process_with_workers(
        state: Arc<State>,
        (mut request, rx): (WorkRequest, oneshot::Receiver<RpcRequestResult>),
    ) -> RpcRequestResult {
        loop {
            match state.workers.try_send(request) {
                Ok(()) => break,
                Err(TrySendError::Full(value)) => {
                    request = value;
                    sleep(Duration::from_micros(100)).await;
                }
                Err(TrySendError::Disconnected(_)) => anyhow::bail!("encode workers disconnected"),
            }
        }

        match rx.await {
            Ok(response) => response,
            Err(_) => anyhow::bail!("failed to get encoded request"),
        }
    }
}

struct RpcRequestBlock {
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    slot: Slot,
    commitment: CommitmentConfig,
    encoding: UiTransactionEncoding,
    encoding_options: BlockEncodingOptions,
}

impl RpcRequestBlock {
    async fn process(self, state: Arc<State>, upstream_disabled: bool) -> RpcRequestResult {
        let deadline = Instant::now() + state.request_timeout;

        // check slot before sending request
        let slot_tip = match self.commitment.commitment {
            CommitmentLevel::Processed => unreachable!(),
            CommitmentLevel::Confirmed => state.stored_slots.confirmed_load(),
            CommitmentLevel::Finalized => state.stored_slots.finalized_load(),
        };
        if self.slot > slot_tip {
            return Self::error_not_available(self.id, self.slot);
        }
        if self.slot <= state.stored_slots.first_available_load() {
            return self
                .fetch_upstream(state, upstream_disabled, deadline)
                .await;
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            state
                .requests_tx
                .send(ReadRequest::Block {
                    deadline,
                    slot: self.slot,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let bytes = match result {
            ReadResultBlock::Timeout => anyhow::bail!("timeout"),
            ReadResultBlock::Removed => {
                return self
                    .fetch_upstream(state, upstream_disabled, deadline)
                    .await;
            }
            ReadResultBlock::Dead => {
                return Self::error_skipped(self.id, self.slot);
            }
            ReadResultBlock::NotAvailable => {
                return Self::error_not_available(self.id, self.slot);
            }
            ReadResultBlock::Block(bytes) => bytes,
            ReadResultBlock::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify that we still have data for that block (i.e. we read correct data)
        if self.slot <= state.stored_slots.first_available_load() {
            return self
                .fetch_upstream(state, upstream_disabled, deadline)
                .await;
        }

        // parse, encode and serialize
        RpcRequest::process_with_workers(state, RpcRequestBlockWorkRequest::create(self, bytes))
            .await
    }

    async fn fetch_upstream(
        self,
        state: Arc<State>,
        upstream_disabled: bool,
        deadline: Instant,
    ) -> RpcRequestResult {
        if let Some(upstream) = (!upstream_disabled)
            .then_some(state.upstream.as_ref())
            .flatten()
        {
            upstream
                .get_block(
                    self.x_subscription_id,
                    deadline,
                    &self.id,
                    self.slot,
                    self.commitment,
                    self.encoding,
                    self.encoding_options,
                )
                .await
        } else {
            Self::error_skipped_long_term_storage(self.id, self.slot)
        }
    }

    fn error_not_available(id: Id<'static>, slot: Slot) -> RpcRequestResult {
        Ok(jsonrpc_response_error_custom(
            id,
            RpcCustomError::BlockNotAvailable { slot },
        ))
    }

    fn error_skipped(id: Id<'static>, slot: Slot) -> RpcRequestResult {
        Ok(jsonrpc_response_error_custom(
            id,
            RpcCustomError::SlotSkipped { slot },
        ))
    }

    fn error_skipped_long_term_storage(id: Id<'static>, slot: Slot) -> RpcRequestResult {
        Ok(jsonrpc_response_error_custom(
            id,
            RpcCustomError::LongTermStorageSlotSkipped { slot },
        ))
    }
}

pub struct RpcRequestBlockWorkRequest {
    request: RpcRequestBlock,
    bytes: Vec<u8>,
    tx: Option<oneshot::Sender<RpcRequestResult>>,
}

impl RpcRequestBlockWorkRequest {
    fn create(
        request: RpcRequestBlock,
        bytes: Vec<u8>,
    ) -> (WorkRequest, oneshot::Receiver<RpcRequestResult>) {
        let (tx, rx) = oneshot::channel();
        let this = Self {
            request,
            bytes,
            tx: Some(tx),
        };
        (WorkRequest::Block(this), rx)
    }

    pub fn process(mut self) {
        if let Some(tx) = self.tx.take() {
            let ts = quanta::Instant::now();
            let _ = tx.send(Self::process2(
                self.bytes,
                self.request.id,
                self.request.slot,
                self.request.encoding,
                self.request.encoding_options,
            ));
            gauge!(
                RPC_WORKERS_CPU_SECONDS_TOTAL,
                "x_subscription_id" => self.request.x_subscription_id,
                "method" => "getBlock"
            )
            .increment(duration_to_seconds(ts.elapsed()));
        }
    }

    fn process2(
        bytes: Vec<u8>,
        id: Id<'static>,
        slot: Slot,
        encoding: UiTransactionEncoding,
        encoding_options: BlockEncodingOptions,
    ) -> RpcRequestResult {
        // parse
        let block = match generated::ConfirmedBlock::decode(bytes.as_ref()) {
            Ok(block) => match ConfirmedBlock::try_from(block) {
                Ok(block) => block,
                Err(error) => {
                    error!(slot, ?error, "failed to decode block");
                    anyhow::bail!("failed to decode block")
                }
            },
            Err(error) => {
                error!(slot, ?error, "failed to decode block protobuf / bincode");
                anyhow::bail!("failed to decode block protobuf / bincode")
            }
        };

        // encode
        let block = match block.encode_with_options(encoding, encoding_options) {
            Ok(block) => block,
            Err(error) => {
                return Ok(jsonrpc_response_error_custom(
                    id,
                    RpcCustomError::from(error),
                ));
            }
        };

        // serialize
        let data = serde_json::to_value(&block).expect("json serialization never fail");

        Ok(jsonrpc_response_success(id, data))
    }
}

#[derive(Debug)]
struct RpcRequestBlockHeight {
    id: Id<'static>,
    commitment: CommitmentConfig,
}

impl RpcRequestBlockHeight {
    async fn process(self, state: Arc<State>) -> RpcRequestResult {
        let deadline = Instant::now() + state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            state
                .requests_tx
                .send(ReadRequest::BlockHeight {
                    deadline,
                    commitment: self.commitment,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let block_height = match result {
            ReadResultBlockHeight::Timeout => anyhow::bail!("timeout"),
            ReadResultBlockHeight::BlockHeight(block_height) => block_height,
            ReadResultBlockHeight::ReadError(error) => anyhow::bail!("read error: {error}"),
        };
        Ok(jsonrpc_response_success(
            self.id,
            serde_json::json!(block_height),
        ))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RpcRequestBlocksUntil {
    EndSlot(Slot),
    Limit(usize),
}

#[derive(Debug)]
struct RpcRequestBlocks {
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    start_slot: Slot,
    until: RpcRequestBlocksUntil,
    commitment: CommitmentConfig,
}

impl RpcRequestBlocks {
    async fn process(self, state: Arc<State>, upstream_disabled: bool) -> RpcRequestResult {
        let deadline = Instant::now() + state.request_timeout;

        // some slot will be removed while we pass request, send to upstream
        let first_available_slot = state.stored_slots.first_available_load() + 32;
        if self.start_slot < first_available_slot {
            if let Some(value) = self
                .fetch_upstream(&state, upstream_disabled, deadline)
                .await
            {
                return value;
            }
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            state
                .requests_tx
                .send(ReadRequest::Blocks {
                    deadline,
                    start_slot: self.start_slot,
                    until: self.until,
                    commitment: self.commitment,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        match result {
            ReadResultBlocks::Timeout => anyhow::bail!("timeout"),
            ReadResultBlocks::Blocks(blocks) => {
                Ok(jsonrpc_response_success(self.id, blocks.into()))
            }
            ReadResultBlocks::ReadError(error) => anyhow::bail!("read error: {error}"),
        }
    }

    async fn fetch_upstream(
        &self,
        state: &State,
        upstream_disabled: bool,
        deadline: Instant,
    ) -> Option<RpcRequestResult> {
        if let Some(upstream) = (!upstream_disabled)
            .then_some(state.upstream.as_ref())
            .flatten()
        {
            Some(
                upstream
                    .get_blocks(
                        Arc::clone(&self.x_subscription_id),
                        deadline,
                        &self.id,
                        self.start_slot,
                        self.until,
                        self.commitment,
                    )
                    .await,
            )
        } else {
            None
        }
    }
}

#[derive(Debug)]
struct RpcRequestBlockTime {
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    slot: Slot,
}

impl RpcRequestBlockTime {
    async fn process(self, state: Arc<State>, upstream_disabled: bool) -> RpcRequestResult {
        let deadline = Instant::now() + state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            state
                .requests_tx
                .send(ReadRequest::BlockTime {
                    deadline,
                    slot: self.slot,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let response = match result {
            ReadResultBlockTime::Timeout => anyhow::bail!("timeout"),
            ReadResultBlockTime::Removed => {
                return self
                    .fetch_upstream(state, upstream_disabled, deadline)
                    .await;
            }
            ReadResultBlockTime::Dead => Err(RpcCustomError::SlotSkipped { slot: self.slot }),
            ReadResultBlockTime::NotAvailable => {
                Err(RpcCustomError::BlockNotAvailable { slot: self.slot })
            }
            ReadResultBlockTime::BlockTime(block_time) => Ok(block_time.into()),
            ReadResultBlockTime::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        Ok(match response {
            Ok(payload) => jsonrpc_response_success(self.id, payload),
            Err(error) => jsonrpc_response_error_custom(self.id, error),
        })
    }

    async fn fetch_upstream(
        self,
        state: Arc<State>,
        upstream_disabled: bool,
        deadline: Instant,
    ) -> RpcRequestResult {
        if let Some(upstream) = (!upstream_disabled)
            .then_some(state.upstream.as_ref())
            .flatten()
        {
            upstream
                .get_block_time(self.x_subscription_id, deadline, &self.id, self.slot)
                .await
        } else {
            Ok(jsonrpc_response_success(
                self.id,
                serde_json::json!(None::<()>),
            ))
        }
    }
}

#[derive(Debug)]
struct RpcRequestLatestBlockhash {
    id: Id<'static>,
    commitment: CommitmentConfig,
}

impl RpcRequestLatestBlockhash {
    async fn process(self, state: Arc<State>) -> RpcRequestResult {
        let deadline = Instant::now() + state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            state
                .requests_tx
                .send(ReadRequest::LatestBlockhash {
                    deadline,
                    commitment: self.commitment,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        match result {
            ReadResultLatestBlockhash::Timeout => anyhow::bail!("timeout"),
            ReadResultLatestBlockhash::LatestBlockhash {
                slot,
                blockhash,
                last_valid_block_height,
            } => {
                let data = serde_json::to_value(&solana_rpc_client_api::response::Response {
                    context: RpcResponseContext::new(slot),
                    value: RpcBlockhash {
                        blockhash,
                        last_valid_block_height,
                    },
                })
                .expect("json serialization never fail");
                Ok(jsonrpc_response_success(self.id, data))
            }
            ReadResultLatestBlockhash::ReadError(error) => {
                anyhow::bail!("read error: {error}")
            }
        }
    }
}

#[derive(Debug)]
struct RpcRequestRecentPrioritizationFees {
    id: Id<'static>,
    pubkeys: Vec<Pubkey>,
    percentile: Option<u16>,
}

impl RpcRequestRecentPrioritizationFees {
    async fn process(self, state: Arc<State>) -> RpcRequestResult {
        let deadline = Instant::now() + state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            state
                .requests_tx
                .send(ReadRequest::RecentPrioritizationFees {
                    deadline,
                    pubkeys: self.pubkeys,
                    percentile: self.percentile,
                    tx,
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        match result {
            ReadResultRecentPrioritizationFees::Timeout => anyhow::bail!("timeout"),
            ReadResultRecentPrioritizationFees::Fees(fees) => {
                let data = serde_json::to_value(&fees).expect("json serialization never fail");
                Ok(jsonrpc_response_success(self.id, data))
            }
        }
    }
}

#[derive(Debug)]
struct RpcRequestSignaturesForAddress {
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    commitment: CommitmentConfig,
    address: Pubkey,
    before: Option<Signature>,
    until: Option<Signature>,
    limit: usize,
}

impl RpcRequestSignaturesForAddress {
    async fn process(self, state: Arc<State>, upstream_disabled: bool) -> RpcRequestResult {
        let deadline = Instant::now() + state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            state
                .requests_tx
                .send(ReadRequest::SignaturesForAddress {
                    deadline,
                    commitment: self.commitment,
                    address: self.address,
                    before: self.before,
                    until: self.until,
                    limit: self.limit,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let (mut signatures, finished, mut before) = match result {
            ReadResultSignaturesForAddress::Timeout => anyhow::bail!("timeout"),
            ReadResultSignaturesForAddress::Signatures {
                signatures,
                finished,
                before,
            } => (signatures, finished, before),
            ReadResultSignaturesForAddress::ReadError(error) => {
                anyhow::bail!("read error: {error}")
            }
        };

        let limit = self.limit - signatures.len();
        let id = if !finished && !upstream_disabled && limit > 0 {
            if !signatures.is_empty() {
                before = signatures
                    .last()
                    .map(|sig| sig.signature.parse().expect("valid sig"));
            }

            match self.fetch_upstream(state, deadline, before, limit).await? {
                Ok((id, mut sigs)) => {
                    signatures.append(&mut sigs);
                    id
                }
                Err(error) => return Ok(error),
            }
        } else {
            self.id
        };

        let data = serde_json::to_value(&signatures).expect("json serialization never fail");
        Ok(jsonrpc_response_success(id, data))
    }

    async fn fetch_upstream(
        self,
        state: Arc<State>,
        deadline: Instant,
        before: Option<Signature>,
        limit: usize,
    ) -> anyhow::Result<
        Result<
            (Id<'static>, Vec<RpcConfirmedTransactionStatusWithSignature>),
            Response<'static, serde_json::Value>,
        >,
    > {
        if let Some(upstream) = state.upstream.as_ref() {
            let response = upstream
                .get_signatures_for_address(
                    self.x_subscription_id,
                    deadline,
                    &self.id,
                    self.address,
                    before,
                    self.until,
                    limit,
                    self.commitment,
                )
                .await?;

            if let ResponsePayload::Error(_) = &response.payload {
                return Ok(Err(response));
            }

            let ResponsePayload::Success(value) = response.payload else {
                unreachable!();
            };
            serde_json::from_value(value.into_owned())
                .context("failed to parse upstream response")
                .map(|vec| Ok((self.id, vec)))
        } else {
            Ok(Ok((self.id, vec![])))
        }
    }
}

#[derive(Debug)]
struct RpcRequestsSignatureStatuses {
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    signatures: Vec<Signature>,
    search_transaction_history: bool,
}

impl RpcRequestsSignatureStatuses {
    async fn process(self, state: Arc<State>, upstream_disabled: bool) -> RpcRequestResult {
        let deadline = Instant::now() + state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            state
                .requests_tx
                .send(ReadRequest::SignatureStatuses {
                    deadline,
                    signatures: self.signatures.clone(),
                    search_transaction_history: self.search_transaction_history,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let mut statuses = match result {
            ReadResultSignatureStatuses::Timeout => anyhow::bail!("timeout"),
            ReadResultSignatureStatuses::Signatures(signatures) => signatures,
            ReadResultSignatureStatuses::ReadError(error) => {
                anyhow::bail!("read error: {error}")
            }
        };

        if self.search_transaction_history
            && !upstream_disabled
            && state.upstream.is_some()
            && !statuses.iter().any(|status| status.is_none())
        {
            let mut signatures_history = Vec::new();
            for (signature, status) in self.signatures.iter().zip(statuses.iter()) {
                if status.is_none() {
                    signatures_history.push(signature);
                }
            }

            let mut signatures_upstream = match self
                .fetch_upstream(&state, deadline, signatures_history)
                .await?
            {
                Ok(sigs) => sigs,
                Err(error) => return Ok(error),
            };

            let mut index = 0;
            for status in statuses.iter_mut() {
                if status.is_none() {
                    *status = signatures_upstream[index].take();
                    index += 1;
                }
            }
        }

        let data = serde_json::to_value(&solana_rpc_client_api::response::Response {
            context: RpcResponseContext::new(state.stored_slots.processed_load()),
            value: statuses,
        })
        .expect("json serialization never fail");
        Ok(jsonrpc_response_success(self.id, data))
    }

    async fn fetch_upstream(
        &self,
        state: &State,
        deadline: Instant,
        signatures: Vec<&Signature>,
    ) -> anyhow::Result<Result<Vec<Option<TransactionStatus>>, Response<'static, serde_json::Value>>>
    {
        if let Some(upstream) = state.upstream.as_ref() {
            let response = upstream
                .get_signature_statuses(
                    Arc::clone(&self.x_subscription_id),
                    deadline,
                    &self.id,
                    signatures,
                )
                .await?;

            if let ResponsePayload::Error(_) = &response.payload {
                return Ok(Err(response));
            }

            let ResponsePayload::Success(value) = response.payload else {
                unreachable!();
            };
            serde_json::from_value(value.into_owned())
                .context("failed to parse upstream response")
                .map(Ok)
        } else {
            Ok(Ok(vec![]))
        }
    }
}

#[derive(Debug)]
struct RpcRequestTransaction {
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    signature: Signature,
    commitment: CommitmentConfig,
    encoding: UiTransactionEncoding,
    max_supported_transaction_version: Option<u8>,
}

impl RpcRequestTransaction {
    async fn process(self, state: Arc<State>, upstream_disabled: bool) -> RpcRequestResult {
        let deadline = Instant::now() + state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            state
                .requests_tx
                .send(ReadRequest::Transaction {
                    deadline,
                    signature: self.signature,
                    tx,
                    x_subscription_id: Arc::clone(&self.x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let (slot, block_time, bytes) = match result {
            ReadResultTransaction::Timeout => anyhow::bail!("timeout"),
            ReadResultTransaction::NotFound => {
                return self
                    .fetch_upstream(state, upstream_disabled, deadline)
                    .await;
            }
            ReadResultTransaction::Transaction {
                slot,
                block_time,
                bytes,
            } => (slot, block_time, bytes),
            ReadResultTransaction::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify commitment
        if self.commitment.is_finalized() && state.stored_slots.finalized_load() < slot {
            return Ok(jsonrpc_response_success(
                self.id,
                serde_json::json!(None::<()>),
            ));
        }

        // verify that we still have data for that block (i.e. we read correct data)
        if slot <= state.stored_slots.first_available_load() {
            return self
                .fetch_upstream(state, upstream_disabled, deadline)
                .await;
        }

        // parse, encode and serialize
        RpcRequest::process_with_workers(
            state,
            RpcRequestTransactionWorkRequest::create(self, slot, block_time, bytes),
        )
        .await
    }

    async fn fetch_upstream(
        self,
        state: Arc<State>,
        upstream_disabled: bool,
        deadline: Instant,
    ) -> RpcRequestResult {
        if let Some(upstream) = (!upstream_disabled)
            .then_some(state.upstream.as_ref())
            .flatten()
        {
            upstream
                .get_transaction(
                    self.x_subscription_id,
                    deadline,
                    &self.id,
                    self.signature,
                    self.commitment,
                    self.encoding,
                    self.max_supported_transaction_version,
                )
                .await
        } else {
            Ok(jsonrpc_response_error_custom(
                self.id,
                RpcCustomError::TransactionHistoryNotAvailable,
            ))
        }
    }
}

#[derive(Debug)]
pub struct RpcRequestTransactionWorkRequest {
    request: RpcRequestTransaction,
    slot: Slot,
    block_time: Option<UnixTimestamp>,
    bytes: Vec<u8>,
    tx: Option<oneshot::Sender<RpcRequestResult>>,
}

impl RpcRequestTransactionWorkRequest {
    fn create(
        request: RpcRequestTransaction,
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        bytes: Vec<u8>,
    ) -> (WorkRequest, oneshot::Receiver<RpcRequestResult>) {
        let (tx, rx) = oneshot::channel();
        let this = Self {
            request,
            slot,
            block_time,
            bytes,
            tx: Some(tx),
        };
        (WorkRequest::Transaction(this), rx)
    }

    pub fn process(mut self) {
        if let Some(tx) = self.tx.take() {
            let ts = quanta::Instant::now();
            let _ = tx.send(Self::process2(
                self.bytes,
                self.slot,
                self.block_time,
                self.request.id,
                self.request.encoding,
                self.request.max_supported_transaction_version,
            ));
            gauge!(
                RPC_WORKERS_CPU_SECONDS_TOTAL,
                "x_subscription_id" => self.request.x_subscription_id,
                "method" => "getTransaction"
            )
            .increment(duration_to_seconds(ts.elapsed()));
        }
    }

    fn process2(
        bytes: Vec<u8>,
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        id: Id<'static>,
        encoding: UiTransactionEncoding,
        max_supported_transaction_version: Option<u8>,
    ) -> RpcRequestResult {
        // parse
        let tx_with_meta = match generated::ConfirmedTransaction::decode(bytes.as_ref()) {
            Ok(tx) => match TransactionWithStatusMeta::try_from(tx) {
                Ok(tx_with_meta) => tx_with_meta,
                Err(error) => {
                    error!(slot, ?error, "failed to decode transaction");
                    anyhow::bail!("failed to decode transaction")
                }
            },
            Err(error) => {
                error!(
                    slot,
                    ?error,
                    "failed to decode transaction protobuf / bincode"
                );
                anyhow::bail!("failed to decode transaction protobuf / bincode")
            }
        };

        // encode
        let confirmed_tx = ConfirmedTransactionWithStatusMeta {
            slot,
            tx_with_meta,
            block_time,
        };
        let tx = match confirmed_tx.encode(encoding, max_supported_transaction_version) {
            Ok(tx) => tx,
            Err(error) => {
                return Ok(jsonrpc_response_error_custom(
                    id,
                    RpcCustomError::from(error),
                ));
            }
        };

        // serialize
        let data = serde_json::to_value(&tx).expect("json serialization never fail");

        Ok(jsonrpc_response_success(id, data))
    }
}

#[derive(Debug)]
pub struct RpcRequestIsBlockhashValid {
    id: Id<'static>,
    blockhash: String,
    commitment: CommitmentConfig,
}

impl RpcRequestIsBlockhashValid {
    async fn process(self, state: Arc<State>) -> RpcRequestResult {
        let deadline = Instant::now() + state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            state
                .requests_tx
                .send(ReadRequest::BlockhashValid {
                    deadline,
                    blockhash: self.blockhash,
                    commitment: self.commitment,
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };

        match result {
            ReadResultBlockhashValid::Timeout => anyhow::bail!("timeout"),
            ReadResultBlockhashValid::Blockhash { slot, is_valid } => {
                let data = serde_json::to_value(&solana_rpc_client_api::response::Response {
                    context: RpcResponseContext::new(slot),
                    value: is_valid,
                })
                .expect("json serialization never fail");
                Ok(jsonrpc_response_success(self.id, data))
            }
            ReadResultBlockhashValid::ReadError(error) => {
                anyhow::bail!("read error: {error}")
            }
        }
    }
}
