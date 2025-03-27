use {
    crate::{
        config::{ConfigRpc, ConfigRpcCall},
        rpc::{upstream::RpcClient, workers::WorkRequest},
        storage::{
            read::{ReadRequest, ReadResultGetBlock, ReadResultGetTransaction},
            slots::StoredSlots,
        },
    },
    crossbeam::channel::{Sender, TrySendError},
    futures::stream::{FuturesOrdered, StreamExt},
    http_body_util::{BodyExt, Full as BodyFull, Limited, combinators::BoxBody},
    hyper::{
        StatusCode,
        body::{Bytes, Incoming as BodyIncoming},
        header::CONTENT_TYPE,
        http::Result as HttpResult,
    },
    jsonrpsee_types::{
        ErrorCode, ErrorObject, ErrorObjectOwned, Id, Params, Request, Response, ResponsePayload,
        TwoPointZero,
    },
    prost::Message,
    serde::{Deserialize, de},
    solana_rpc_client_api::{
        config::{
            RpcBlockConfig, RpcContextConfig, RpcEncodingConfigWrapper, RpcTransactionConfig,
        },
        custom_error::RpcCustomError,
    },
    solana_sdk::{
        clock::{Slot, UnixTimestamp},
        commitment_config::{CommitmentConfig, CommitmentLevel},
        signature::Signature,
    },
    solana_storage_proto::convert::generated,
    solana_transaction_status::{
        BlockEncodingOptions, ConfirmedBlock, ConfirmedTransactionWithStatusMeta,
        TransactionWithStatusMeta, UiTransactionEncoding,
    },
    std::{
        fmt,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{
        sync::{mpsc, oneshot},
        time::sleep,
    },
    tracing::error,
};

type RpcResponse = hyper::Response<BoxBody<Bytes, std::convert::Infallible>>;

type RpcRequestResult = anyhow::Result<Response<'static, serde_json::Value>>;

fn response_200<D: Into<Bytes>>(data: D) -> HttpResult<RpcResponse> {
    hyper::Response::builder()
        .header(CONTENT_TYPE, "application/json; charset=utf-8")
        .body(BodyFull::from(data.into()).boxed())
}

fn response_400<E: fmt::Display>(error: E) -> HttpResult<RpcResponse> {
    hyper::Response::builder()
        .status(StatusCode::BAD_REQUEST)
        .body(format!("{error}\n").boxed())
}

fn response_500<E: fmt::Display>(error: E) -> HttpResult<RpcResponse> {
    hyper::Response::builder()
        .status(StatusCode::INTERNAL_SERVER_ERROR)
        .body(format!("{error}\n").boxed())
}

fn jsonrpc_response_error(
    id: Id<'static>,
    error: RpcCustomError,
) -> Response<'static, serde_json::Value> {
    let error = jsonrpc_core::Error::from(error);
    Response {
        jsonrpc: Some(TwoPointZero),
        payload: ResponsePayload::error(ErrorObject::owned(
            error.code.code() as i32,
            error.message,
            error.data,
        )),
        id,
    }
}

#[derive(Debug, Clone, Copy)]
struct SupportedCalls {
    get_block: bool,
    get_slot: bool,
    get_transaction: bool,
}

impl SupportedCalls {
    fn new(calls: &[ConfigRpcCall]) -> anyhow::Result<Self> {
        Ok(Self {
            get_block: Self::check_call_support(calls, ConfigRpcCall::GetBlock)?,
            get_slot: Self::check_call_support(calls, ConfigRpcCall::GetSlot)?,
            get_transaction: Self::check_call_support(calls, ConfigRpcCall::GetTransaction)?,
        })
    }

    fn check_call_support(calls: &[ConfigRpcCall], call: ConfigRpcCall) -> anyhow::Result<bool> {
        let count = calls.iter().filter(|value| **value == call).count();
        anyhow::ensure!(count <= 1, "{call:?} defined multiple times");
        Ok(count == 1)
    }
}

#[derive(Debug)]
pub struct State {
    stored_slots: StoredSlots,
    body_limit: usize,
    request_timeout: Duration,
    supported_calls: SupportedCalls,
    requests_tx: mpsc::Sender<ReadRequest>,
    upstream: Option<RpcClient>,
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
            supported_calls: SupportedCalls::new(&config.calls)?,
            requests_tx,
            upstream: config.upstream.map(RpcClient::new).transpose()?,
            workers,
        })
    }

    pub fn is_ready(&self) -> bool {
        self.stored_slots.is_ready()
    }
}

pub async fn on_request(
    req: hyper::Request<BodyIncoming>,
    state: Arc<State>,
) -> HttpResult<RpcResponse> {
    let (parts, body) = req.into_parts();

    // Same name as in Agave Rpc
    let upstream_disabled = parts
        .headers
        .get("x-bigtable")
        .is_some_and(|v| v == "disabled");

    let bytes = match Limited::new(body, state.body_limit).collect().await {
        Ok(body) => body.to_bytes(),
        Err(error) => return response_400(error),
    };

    let requests = match RpcRequests::parse(&bytes) {
        Ok(requests) => requests,
        Err(error) => return response_400(error),
    };
    let mut buffer = match requests {
        RpcRequests::Single(request) => match RpcRequest::parse(request, &state) {
            Ok(request) => match request.process(Arc::clone(&state), upstream_disabled).await {
                Ok(response) => {
                    serde_json::to_vec(&response).expect("json serialization never fail")
                }
                Err(error) => return response_500(error),
            },
            Err(error) => serde_json::to_vec(&error).expect("json serialization never fail"),
        },
        RpcRequests::Batch(requests) => {
            let mut futures = FuturesOrdered::new();
            for request in requests {
                let state = state.clone();
                futures.push_back(async move {
                    match RpcRequest::parse(request, &state) {
                        Ok(request) => request.process(state, upstream_disabled).await,
                        Err(error) => Ok(error),
                    }
                });
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

#[allow(clippy::enum_variant_names)]
enum RpcRequest {
    GetBlock(RpcRequestGetBlock),
    GetSlot { response: RpcRequestResult },
    GetTransaction(RpcRequestGetTransaction),
    // Remove clippy rule
    // IsBlockhashValid,
}

impl RpcRequest {
    fn parse<'a>(
        request: Request<'a>,
        state: &Arc<State>,
    ) -> Result<Self, Response<'a, serde_json::Value>> {
        match request.method.as_ref() {
            "getBlock" if state.supported_calls.get_block => {
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
                    return Err(Self::response_error(id, error));
                }

                Ok(Self::GetBlock(RpcRequestGetBlock {
                    id: id.into_owned(),
                    slot,
                    commitment,
                    encoding,
                    encoding_options,
                }))
            }
            "getSlot" if state.supported_calls.get_slot => {
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

                let slot = match commitment.unwrap_or_default().commitment {
                    CommitmentLevel::Processed => state.stored_slots.processed_load(),
                    CommitmentLevel::Confirmed => state.stored_slots.confirmed_load(),
                    CommitmentLevel::Finalized => state.stored_slots.finalized_load(),
                };

                if let Some(min_context_slot) = min_context_slot {
                    if slot < min_context_slot {
                        return Ok(Self::GetSlot {
                            response: Ok(jsonrpc_response_error(
                                id.into_owned(),
                                RpcCustomError::MinContextSlotNotReached { context_slot: slot },
                            )),
                        });
                    }
                }

                Ok(Self::GetSlot {
                    response: Self::response_success(id.into_owned(), slot.into()),
                })
            }
            "getTransaction" if state.supported_calls.get_transaction => {
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
                    Err(error) => return Err(Self::response_error(id, error)),
                };

                let config = config
                    .map(|config| config.convert_to_current())
                    .unwrap_or_default();
                let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
                let max_supported_transaction_version = config.max_supported_transaction_version;
                let commitment = config.commitment.unwrap_or_default();
                if let Err(error) = Self::check_is_at_least_confirmed(commitment) {
                    return Err(Self::response_error(id, error));
                }

                Ok(Self::GetTransaction(RpcRequestGetTransaction {
                    id: id.into_owned(),
                    signature,
                    commitment,
                    encoding,
                    max_supported_transaction_version,
                }))
            }
            _ => Err(Response {
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
            return Err(ErrorObject::borrowed(
                ErrorCode::InvalidParams.code(),
                "Method does not support commitment below `confirmed`",
                None,
            ));
        }
        Ok(())
    }

    fn verify_signature(input: &str) -> Result<Signature, ErrorObjectOwned> {
        input.parse().map_err(|error| {
            ErrorObject::owned::<()>(
                ErrorCode::InvalidParams.code(),
                format!("Invalid param: {error:?}"),
                None,
            )
        })
    }

    fn response_error(id: Id<'_>, error: ErrorObjectOwned) -> Response<'_, serde_json::Value> {
        Response {
            jsonrpc: Some(TwoPointZero),
            payload: ResponsePayload::error(error),
            id,
        }
    }

    fn response_success(id: Id<'static>, payload: serde_json::Value) -> RpcRequestResult {
        Ok(Response {
            jsonrpc: Some(TwoPointZero),
            payload: ResponsePayload::success(payload),
            id,
        })
    }

    async fn process(self, state: Arc<State>, upstream_disabled: bool) -> RpcRequestResult {
        match self {
            Self::GetBlock(request) => request.process(state, upstream_disabled).await,
            Self::GetSlot { response } => response,
            Self::GetTransaction(request) => request.process(state, upstream_disabled).await,
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

struct RpcRequestGetBlock {
    id: Id<'static>,
    slot: Slot,
    commitment: CommitmentConfig,
    encoding: UiTransactionEncoding,
    encoding_options: BlockEncodingOptions,
}

impl RpcRequestGetBlock {
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
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let bytes = match result {
            ReadResultGetBlock::Timeout => anyhow::bail!("timeout"),
            ReadResultGetBlock::Removed => {
                return self
                    .fetch_upstream(state, upstream_disabled, deadline)
                    .await;
            }
            ReadResultGetBlock::Dead => {
                return Self::error_skipped(self.id, self.slot);
            }
            ReadResultGetBlock::NotAvailable => {
                return Self::error_not_available(self.id, self.slot);
            }
            ReadResultGetBlock::Block(bytes) => bytes,
            ReadResultGetBlock::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify that we still have data for that block (i.e. we read correct data)
        if self.slot <= state.stored_slots.first_available_load() {
            return self
                .fetch_upstream(state, upstream_disabled, deadline)
                .await;
        }

        // parse, encode and serialize
        RpcRequest::process_with_workers(state, RpcRequestGetBlockWorkRequest::create(self, bytes))
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
                    deadline,
                    self.id,
                    self.slot,
                    self.commitment,
                    self.encoding,
                    self.encoding_options,
                )
                .await
        } else {
            Self::error_skipped(self.id, self.slot)
        }
    }

    fn error_not_available(id: Id<'static>, slot: Slot) -> RpcRequestResult {
        Ok(jsonrpc_response_error(
            id,
            RpcCustomError::BlockNotAvailable { slot },
        ))
    }

    fn error_skipped(id: Id<'static>, slot: Slot) -> RpcRequestResult {
        Ok(jsonrpc_response_error(
            id,
            RpcCustomError::LongTermStorageSlotSkipped { slot },
        ))
    }
}

pub struct RpcRequestGetBlockWorkRequest {
    bytes: Vec<u8>,
    id: Id<'static>,
    slot: Slot,
    encoding: UiTransactionEncoding,
    encoding_options: BlockEncodingOptions,
    tx: Option<oneshot::Sender<RpcRequestResult>>,
}

impl RpcRequestGetBlockWorkRequest {
    fn create(
        request: RpcRequestGetBlock,
        bytes: Vec<u8>,
    ) -> (WorkRequest, oneshot::Receiver<RpcRequestResult>) {
        let (tx, rx) = oneshot::channel();
        let this = Self {
            bytes,
            id: request.id,
            slot: request.slot,
            encoding: request.encoding,
            encoding_options: request.encoding_options,
            tx: Some(tx),
        };
        (WorkRequest::Block(this), rx)
    }

    pub fn process(mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(self.process2());
        }
    }

    fn process2(self) -> RpcRequestResult {
        // parse
        let block = match generated::ConfirmedBlock::decode(self.bytes.as_ref()) {
            Ok(block) => match ConfirmedBlock::try_from(block) {
                Ok(block) => block,
                Err(error) => {
                    error!(self.slot, ?error, "failed to decode block");
                    anyhow::bail!("failed to decode block")
                }
            },
            Err(error) => {
                error!(
                    self.slot,
                    ?error,
                    "failed to decode block protobuf / bincode"
                );
                anyhow::bail!("failed to decode block protobuf / bincode")
            }
        };

        // encode
        let block = match block.encode_with_options(self.encoding, self.encoding_options) {
            Ok(block) => block,
            Err(error) => {
                return Ok(jsonrpc_response_error(self.id, RpcCustomError::from(error)));
            }
        };

        // serialize
        let data = serde_json::to_value(&block).expect("json serialization never fail");

        RpcRequest::response_success(self.id, data)
    }
}

struct RpcRequestGetTransaction {
    id: Id<'static>,
    signature: Signature,
    commitment: CommitmentConfig,
    encoding: UiTransactionEncoding,
    max_supported_transaction_version: Option<u8>,
}

impl RpcRequestGetTransaction {
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
                    tx
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let (slot, block_time, bytes) = match result {
            ReadResultGetTransaction::Timeout => anyhow::bail!("timeout"),
            ReadResultGetTransaction::NotFound => {
                return self
                    .fetch_upstream(state, upstream_disabled, deadline)
                    .await;
            }
            ReadResultGetTransaction::Transaction {
                slot,
                block_time,
                bytes,
            } => (slot, block_time, bytes),
            ReadResultGetTransaction::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify commitment
        if self.commitment.is_finalized() && state.stored_slots.finalized_load() < slot {
            return RpcRequest::response_success(self.id, serde_json::json!(None::<()>));
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
            RpcRequestGetTransactionWorkRequest::create(self, slot, block_time, bytes),
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
                    deadline,
                    self.id,
                    self.signature,
                    self.commitment,
                    self.encoding,
                    self.max_supported_transaction_version,
                )
                .await
        } else {
            Ok(jsonrpc_response_error(
                self.id,
                RpcCustomError::TransactionHistoryNotAvailable,
            ))
        }
    }
}

pub struct RpcRequestGetTransactionWorkRequest {
    slot: Slot,
    block_time: Option<UnixTimestamp>,
    bytes: Vec<u8>,
    id: Id<'static>,
    encoding: UiTransactionEncoding,
    max_supported_transaction_version: Option<u8>,
    tx: Option<oneshot::Sender<RpcRequestResult>>,
}

impl RpcRequestGetTransactionWorkRequest {
    fn create(
        request: RpcRequestGetTransaction,
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        bytes: Vec<u8>,
    ) -> (WorkRequest, oneshot::Receiver<RpcRequestResult>) {
        let (tx, rx) = oneshot::channel();
        let this = Self {
            slot,
            block_time,
            bytes,
            id: request.id,
            encoding: request.encoding,
            max_supported_transaction_version: request.max_supported_transaction_version,
            tx: Some(tx),
        };
        (WorkRequest::Transaction(this), rx)
    }

    pub fn process(mut self) {
        if let Some(tx) = self.tx.take() {
            let _ = tx.send(self.process2());
        }
    }

    fn process2(self) -> RpcRequestResult {
        // parse
        let tx_with_meta = match generated::ConfirmedTransaction::decode(self.bytes.as_ref()) {
            Ok(tx) => match TransactionWithStatusMeta::try_from(tx) {
                Ok(tx_with_meta) => tx_with_meta,
                Err(error) => {
                    error!(self.slot, ?error, "failed to decode transaction");
                    anyhow::bail!("failed to decode transaction")
                }
            },
            Err(error) => {
                error!(
                    self.slot,
                    ?error,
                    "failed to decode transaction protobuf / bincode"
                );
                anyhow::bail!("failed to decode transaction protobuf / bincode")
            }
        };

        // encode
        let confirmed_tx = ConfirmedTransactionWithStatusMeta {
            slot: self.slot,
            tx_with_meta,
            block_time: self.block_time,
        };
        let tx = match confirmed_tx.encode(self.encoding, self.max_supported_transaction_version) {
            Ok(tx) => tx,
            Err(error) => {
                return Ok(jsonrpc_response_error(self.id, RpcCustomError::from(error)));
            }
        };

        // serialize
        let data = serde_json::to_value(&tx).expect("json serialization never fail");

        RpcRequest::response_success(self.id, data)
    }
}
