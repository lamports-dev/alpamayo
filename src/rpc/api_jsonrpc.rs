use {
    crate::{
        config::{ConfigRpc, ConfigRpcCallJson},
        metrics::RPC_WORKERS_CPU_SECONDS_TOTAL,
        rpc::{upstream::RpcClientJsonrpc, workers::WorkRequest},
        storage::{
            read::{
                ReadRequest, ReadResultBlock, ReadResultBlockHeight, ReadResultBlockTime,
                ReadResultBlockhashValid, ReadResultBlocks, ReadResultInflationReward,
                ReadResultLatestBlockhash, ReadResultRecentPrioritizationFees,
                ReadResultSignatureStatuses, ReadResultSignaturesForAddress, ReadResultTransaction,
            },
            rocksdb::{
                InflationRewardBaseValue, ReadRequestResultInflationReward,
                RocksdbWriteInflationReward,
            },
            slots::StoredSlots,
        },
        util::HashMap,
    },
    crossbeam::channel::{Sender, TrySendError},
    futures::future::BoxFuture,
    jsonrpsee_types::{
        Id, Params, Request, Response, ResponsePayload, TwoPointZero,
        error::{ErrorObjectOwned, INVALID_PARAMS_MSG},
    },
    metrics::gauge,
    prost::Message,
    richat_shared::{
        jsonrpc::{
            helpers::{
                jsonrpc_error_invalid_params, jsonrpc_response_error,
                jsonrpc_response_error_custom, jsonrpc_response_success, to_vec,
            },
            requests::{RpcRequestResult, RpcRequestsProcessor},
        },
        metrics::duration_to_seconds,
    },
    serde::{Deserialize, Serialize, de},
    serde_json::json,
    solana_rpc_client_api::{
        config::{
            RpcBlockConfig, RpcBlocksConfigWrapper, RpcContextConfig, RpcEncodingConfigWrapper,
            RpcEpochConfig, RpcLeaderScheduleConfig, RpcLeaderScheduleConfigWrapper,
            RpcSignatureStatusConfig, RpcSignaturesForAddressConfig, RpcTransactionConfig,
        },
        custom_error::RpcCustomError,
        request::{MAX_GET_CONFIRMED_BLOCKS_RANGE, MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS},
        response::{
            Response as RpcResponse, RpcBlockhash, RpcConfirmedTransactionStatusWithSignature,
            RpcInflationReward, RpcResponseContext, RpcVersionInfo,
        },
    },
    solana_sdk::{
        clock::{Epoch, Slot, UnixTimestamp},
        commitment_config::{CommitmentConfig, CommitmentLevel},
        epoch_rewards_hasher::EpochRewardsHasher,
        epoch_schedule::EpochSchedule,
        hash::Hash,
        pubkey::Pubkey,
        signature::Signature,
        transaction::MAX_TX_ACCOUNT_LOCKS,
    },
    solana_storage_proto::convert::generated,
    solana_transaction_status::{
        BlockEncodingOptions, ConfirmedBlock, ConfirmedTransactionWithStatusMeta, Reward,
        RewardType, TransactionDetails, TransactionStatus, TransactionWithStatusMeta,
        UiConfirmedBlock, UiTransactionEncoding,
    },
    std::{
        collections::HashSet,
        future::Future,
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

#[derive(Debug)]
pub struct State {
    epoch_schedule: EpochSchedule,
    stored_slots: StoredSlots,
    request_timeout: Duration,
    gsfa_limit: usize,
    gss_transaction_history: bool,
    grpf_percentile: bool,
    requests_tx: mpsc::Sender<ReadRequest>,
    db_write_inflation_reward: RocksdbWriteInflationReward,
    upstream: Option<RpcClientJsonrpc>,
    workers: Sender<WorkRequest>,
}

impl State {
    pub fn new(
        config: ConfigRpc,
        stored_slots: StoredSlots,
        requests_tx: mpsc::Sender<ReadRequest>,
        db_write_inflation_reward: RocksdbWriteInflationReward,
        workers: Sender<WorkRequest>,
    ) -> anyhow::Result<Self> {
        let upstream = config
            .upstream_jsonrpc
            .map(|config_upstream| RpcClientJsonrpc::new(config_upstream, config.gcn_cache_ttl))
            .transpose()?;

        if config
            .calls_jsonrpc
            .intersection(&HashSet::from([
                ConfigRpcCallJson::GetClusterNodes,
                ConfigRpcCallJson::GetLeaderSchedule,
            ]))
            .next()
            .is_some()
        {
            anyhow::ensure!(
                upstream.is_some(),
                "upstream required for cached methods like `getClusterNodes`"
            );
        }

        Ok(Self {
            epoch_schedule: EpochSchedule::without_warmup(),
            stored_slots,
            request_timeout: config.request_timeout,
            gsfa_limit: config.gsfa_limit,
            gss_transaction_history: config.gss_transaction_history,
            grpf_percentile: config.grpf_percentile,
            requests_tx,
            db_write_inflation_reward,
            upstream,
            workers,
        })
    }
}

pub fn create_request_processor(
    config: ConfigRpc,
    stored_slots: StoredSlots,
    requests_tx: mpsc::Sender<ReadRequest>,
    db_write_inflation_reward: RocksdbWriteInflationReward,
    workers: Sender<WorkRequest>,
) -> anyhow::Result<RpcRequestsProcessor<Arc<State>>> {
    let state = State::new(
        config.clone(),
        stored_slots,
        requests_tx,
        db_write_inflation_reward,
        workers,
    )?;
    let mut processor = RpcRequestsProcessor::new(config.body_limit, Arc::new(state));

    let calls = &config.calls_jsonrpc;
    if calls.contains(&ConfigRpcCallJson::GetBlock) {
        processor.add_handler("getBlock", Box::new(RpcRequestBlock::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetBlockHeight) {
        processor.add_handler("getBlockHeight", Box::new(RpcRequestBlockHeight::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetBlocks) {
        processor.add_handler("getBlocks", Box::new(RpcRequestBlocks::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetBlocksWithLimit) {
        processor.add_handler(
            "getBlocksWithLimit",
            Box::new(RpcRequestBlocksWithLimit::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetBlockTime) {
        processor.add_handler("getBlockTime", Box::new(RpcRequestBlockTime::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetClusterNodes) {
        processor.add_handler("getClusterNodes", Box::new(RpcRequestClusterNodes::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetFirstAvailableBlock) {
        processor.add_handler(
            "getFirstAvailableBlock",
            Box::new(RpcRequestFirstAvailableBlock::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetInflationReward) {
        processor.add_handler(
            "getInflationReward",
            Box::new(RpcRequestInflationReward::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetLatestBlockhash) {
        processor.add_handler(
            "getLatestBlockhash",
            Box::new(RpcRequestLatestBlockhash::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetLeaderSchedule) {
        processor.add_handler(
            "getLeaderSchedule",
            Box::new(RpcRequestLeaderSchedule::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetRecentPrioritizationFees) {
        processor.add_handler(
            "getRecentPrioritizationFees",
            Box::new(RpcRequestRecentPrioritizationFees::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetSignaturesForAddress) {
        processor.add_handler(
            "getSignaturesForAddress",
            Box::new(RpcRequestSignaturesForAddress::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetSignatureStatuses) {
        processor.add_handler(
            "getSignatureStatuses",
            Box::new(RpcRequestSignatureStatuses::handle),
        );
    }
    if calls.contains(&ConfigRpcCallJson::GetSlot) {
        processor.add_handler("getSlot", Box::new(RpcRequestSlot::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetTransaction) {
        processor.add_handler("getTransaction", Box::new(RpcRequestTransaction::handle));
    }
    if calls.contains(&ConfigRpcCallJson::GetVersion) {
        processor.add_handler("getVersion", Box::new(RpcRequestVersion::handle));
    }
    if calls.contains(&ConfigRpcCallJson::IsBlockhashValid) {
        processor.add_handler(
            "isBlockhashValid",
            Box::new(RpcRequestIsBlockhashValid::handle),
        );
    }

    Ok(processor)
}

trait RpcRequestHandler: Sized {
    fn handle(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> BoxFuture<'_, RpcRequestResult>
    where
        Self: Send,
    {
        Box::pin(async move {
            match Self::parse(state, x_subscription_id, upstream_disabled, request) {
                Ok(req) => req.process().await,
                Err(response) => Ok(response),
            }
        })
    }

    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>>;

    fn process(self) -> impl Future<Output = RpcRequestResult> + Send {
        async { unimplemented!() }
    }
}

fn parse_params<'a, T>(request: Request<'a>) -> Result<(Id<'a>, T), Vec<u8>>
where
    T: for<'de> de::Deserialize<'de>,
{
    let params = Params::new(request.params.as_ref().map(|p| p.get()));
    match params.parse() {
        Ok(params) => Ok((request.id, params)),
        Err(error) => Err(to_vec(&Response {
            jsonrpc: Some(TwoPointZero),
            payload: ResponsePayload::<()>::error(error),
            id: request.id,
        })),
    }
}

fn no_params_expected(request: Request<'_>) -> Result<Request<'_>, Vec<u8>> {
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
        Ok(request)
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
) -> Result<(Id<'a>, Option<Slot>), Vec<u8>> {
    if let Some(min_context_slot) = min_context_slot {
        let context_slot = match commitment.commitment {
            CommitmentLevel::Processed => state.stored_slots.processed_load(),
            CommitmentLevel::Confirmed => state.stored_slots.confirmed_load(),
            CommitmentLevel::Finalized => state.stored_slots.finalized_load(),
        };

        if context_slot < min_context_slot {
            Err(jsonrpc_response_error_custom(
                id,
                RpcCustomError::MinContextSlotNotReached { context_slot },
            ))
        } else {
            Ok((id, Some(context_slot)))
        }
    } else {
        Ok((id, None))
    }
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
    let address = verify_pubkey(&address)?;
    let before = before
        .map(|ref before| verify_signature(before))
        .transpose()?;
    let until = until.map(|ref until| verify_signature(until)).transpose()?;
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
    (state, mut request, rx): (Arc<State>, WorkRequest, oneshot::Receiver<RpcRequestResult>),
) -> RpcRequestResult {
    loop {
        match state.workers.try_send(request) {
            Ok(()) => break,
            Err(TrySendError::Full(value)) => {
                request = value;
                sleep(Duration::from_micros(250)).await;
            }
            Err(TrySendError::Disconnected(_)) => anyhow::bail!("encode workers disconnected"),
        }
    }

    match rx.await {
        Ok(response) => response,
        Err(_) => anyhow::bail!("failed to get encoded request"),
    }
}

struct RpcRequestBlock {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    slot: Slot,
    commitment: CommitmentConfig,
    encoding: UiTransactionEncoding,
    encoding_options: BlockEncodingOptions,
}

impl RpcRequestHandler for RpcRequestBlock {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            slot: Slot,
            #[serde(default)]
            config: Option<RpcEncodingConfigWrapper<RpcBlockConfig>>,
        }

        let (id, ReqParams { slot, config }) = parse_params(request)?;

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
        if let Err(error) = check_is_at_least_confirmed(commitment) {
            return Err(jsonrpc_response_error(id, error));
        }

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            slot,
            commitment,
            encoding,
            encoding_options,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // check slot before sending request
        let slot_tip = match self.commitment.commitment {
            CommitmentLevel::Processed => unreachable!(),
            CommitmentLevel::Confirmed => self.state.stored_slots.confirmed_load(),
            CommitmentLevel::Finalized => self.state.stored_slots.finalized_load(),
        };
        if self.slot > slot_tip {
            return Ok(Self::error_not_available(self.id, self.slot));
        }
        if self.slot <= self.state.stored_slots.first_available_load() {
            return self.fetch_upstream(deadline).await;
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
                return self.fetch_upstream(deadline).await;
            }
            ReadResultBlock::Dead => {
                return Ok(Self::error_skipped(self.id, self.slot));
            }
            ReadResultBlock::NotAvailable => {
                return Ok(Self::error_not_available(self.id, self.slot));
            }
            ReadResultBlock::Block(bytes) => bytes,
            ReadResultBlock::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify that we still have data for that block (i.e. we read correct data)
        if self.slot <= self.state.stored_slots.first_available_load() {
            return self.fetch_upstream(deadline).await;
        }

        // parse, encode and serialize
        process_with_workers(RpcRequestBlockWorkRequest::create(self, bytes)).await
    }
}

impl RpcRequestBlock {
    async fn fetch_upstream(self, deadline: Instant) -> RpcRequestResult {
        if let Some(upstream) = (!self.upstream_disabled)
            .then_some(self.state.upstream.as_ref())
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
            Ok(Self::error_skipped_long_term_storage(self.id, self.slot))
        }
    }

    fn error_not_available(id: Id<'static>, slot: Slot) -> Vec<u8> {
        jsonrpc_response_error_custom(id, RpcCustomError::BlockNotAvailable { slot })
    }

    fn error_skipped(id: Id<'static>, slot: Slot) -> Vec<u8> {
        jsonrpc_response_error_custom(id, RpcCustomError::SlotSkipped { slot })
    }

    fn error_skipped_long_term_storage(id: Id<'static>, slot: Slot) -> Vec<u8> {
        jsonrpc_response_error_custom(id, RpcCustomError::LongTermStorageSlotSkipped { slot })
    }
}

pub struct RpcRequestBlockWorkRequest {
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    slot: Slot,
    encoding: UiTransactionEncoding,
    encoding_options: BlockEncodingOptions,
    bytes: Vec<u8>,
    tx: Option<oneshot::Sender<RpcRequestResult>>,
}

impl RpcRequestBlockWorkRequest {
    fn create(
        request: RpcRequestBlock,
        bytes: Vec<u8>,
    ) -> (Arc<State>, WorkRequest, oneshot::Receiver<RpcRequestResult>) {
        let (tx, rx) = oneshot::channel();
        let this = Self {
            x_subscription_id: request.x_subscription_id,
            id: request.id,
            slot: request.slot,
            encoding: request.encoding,
            encoding_options: request.encoding_options,
            bytes,
            tx: Some(tx),
        };
        (request.state, WorkRequest::Block(this), rx)
    }

    pub fn process(mut self) {
        if let Some(tx) = self.tx.take() {
            let ts = quanta::Instant::now();
            let _ = tx.send(Self::process2(
                self.bytes,
                self.id,
                self.slot,
                self.encoding,
                self.encoding_options,
            ));
            gauge!(
                RPC_WORKERS_CPU_SECONDS_TOTAL,
                "x_subscription_id" => self.x_subscription_id,
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
        // parse, encode and serialize
        Ok(
            match Self::parse_and_encode(&bytes, &id, slot, encoding, encoding_options)? {
                Ok(block) => jsonrpc_response_success(id, &block),
                Err(error) => error,
            },
        )
    }

    fn parse_and_encode(
        bytes: &[u8],
        id: &Id<'static>,
        slot: Slot,
        encoding: UiTransactionEncoding,
        encoding_options: BlockEncodingOptions,
    ) -> anyhow::Result<Result<UiConfirmedBlock, Vec<u8>>> {
        // parse
        let block = match generated::ConfirmedBlock::decode(bytes) {
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
        let result = match block.encode_with_options(encoding, encoding_options) {
            Ok(block) => Ok(block),
            Err(error) => Err(jsonrpc_response_error_custom(
                id.clone(),
                RpcCustomError::from(error),
            )),
        };
        Ok(result)
    }
}

#[derive(Debug)]
struct RpcRequestBlockHeight {
    state: Arc<State>,
    id: Id<'static>,
    commitment: CommitmentConfig,
}

impl RpcRequestHandler for RpcRequestBlockHeight {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (id, ReqParams { config }) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
        let RpcContextConfig {
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default();

        let (id, _slot) = min_context_check(id, min_context_slot, commitment, &state)?;

        Ok(Self {
            state,
            id: id.into_owned(),
            commitment,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
            ReadResultBlockHeight::BlockHeight { block_height, .. } => block_height,
            ReadResultBlockHeight::ReadError(error) => anyhow::bail!("read error: {error}"),
        };
        Ok(jsonrpc_response_success(self.id, json!(block_height)))
    }
}

#[derive(Debug, Clone, Copy)]
pub enum RpcRequestBlocksUntil {
    EndSlot(Slot),
    Limit(usize),
}

#[derive(Debug)]
struct RpcRequestBlocks {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    start_slot: Slot,
    until: RpcRequestBlocksUntil,
    commitment: CommitmentConfig,
}

impl RpcRequestHandler for RpcRequestBlocks {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
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
        ) = parse_params(request)?;
        let (end_slot, maybe_config) = wrapper.map(|wrapper| wrapper.unzip()).unwrap_or_default();
        let config = config.or(maybe_config).unwrap_or_default();

        let commitment = config.commitment.unwrap_or_default();
        if let Err(error) = check_is_at_least_confirmed(commitment) {
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
            end_slot.unwrap_or_else(|| start_slot.saturating_add(MAX_GET_CONFIRMED_BLOCKS_RANGE)),
            if commitment.is_finalized() {
                finalized_slot
            } else {
                state.stored_slots.confirmed_load()
            },
        );
        if end_slot < start_slot {
            return Err(jsonrpc_response_success(id, json!([])));
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

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            start_slot,
            until: RpcRequestBlocksUntil::EndSlot(end_slot),
            commitment,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // some slot will be removed while we pass request, send to upstream
        let first_available_slot = self.state.stored_slots.first_available_load() + 32;
        if self.start_slot < first_available_slot
            && let Some(upstream) = (!self.upstream_disabled)
                .then_some(self.state.upstream.as_ref())
                .flatten()
        {
            return upstream
                .get_blocks(
                    Arc::clone(&self.x_subscription_id),
                    deadline,
                    &self.id,
                    self.start_slot,
                    self.until,
                    self.commitment,
                )
                .await;
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
            ReadResultBlocks::Blocks(blocks) => Ok(jsonrpc_response_success(self.id, &blocks)),
            ReadResultBlocks::ReadError(error) => anyhow::bail!("read error: {error}"),
        }
    }
}

#[derive(Debug)]
struct RpcRequestBlocksWithLimit {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    start_slot: Slot,
    until: RpcRequestBlocksUntil,
    commitment: CommitmentConfig,
}

impl RpcRequestHandler for RpcRequestBlocksWithLimit {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
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
        ) = parse_params(request)?;
        let config = config.unwrap_or_default();

        let commitment = config.commitment.unwrap_or_default();
        if let Err(error) = check_is_at_least_confirmed(commitment) {
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
            return Err(jsonrpc_response_success(id, json!([])));
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

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            start_slot,
            until: RpcRequestBlocksUntil::Limit(limit),
            commitment,
        })
    }

    async fn process(self) -> RpcRequestResult {
        RpcRequestBlocks {
            state: self.state,
            x_subscription_id: self.x_subscription_id,
            upstream_disabled: self.upstream_disabled,
            id: self.id,
            start_slot: self.start_slot,
            until: self.until,
            commitment: self.commitment,
        }
        .process()
        .await
    }
}

#[derive(Debug)]
struct RpcRequestBlockTime {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    slot: Slot,
}

impl RpcRequestHandler for RpcRequestBlockTime {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            slot: Slot,
        }

        let (id, ReqParams { slot }) = parse_params(request)?;

        if slot == 0 {
            Err(jsonrpc_response_success(id, 1584368940))
        } else {
            Ok(Self {
                state,
                x_subscription_id,
                upstream_disabled,
                id: id.into_owned(),
                slot,
            })
        }
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
                return self.fetch_upstream(deadline).await;
            }
            ReadResultBlockTime::Dead => Err(RpcCustomError::SlotSkipped { slot: self.slot }),
            ReadResultBlockTime::NotAvailable => {
                Err(RpcCustomError::BlockNotAvailable { slot: self.slot })
            }
            ReadResultBlockTime::BlockTime(block_time) => Ok(block_time),
            ReadResultBlockTime::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        Ok(match response {
            Ok(payload) => jsonrpc_response_success(self.id, payload),
            Err(error) => jsonrpc_response_error_custom(self.id, error),
        })
    }
}

impl RpcRequestBlockTime {
    async fn fetch_upstream(self, deadline: Instant) -> RpcRequestResult {
        if let Some(upstream) = (!self.upstream_disabled)
            .then_some(self.state.upstream.as_ref())
            .flatten()
        {
            upstream
                .get_block_time(self.x_subscription_id, deadline, &self.id, self.slot)
                .await
        } else {
            Ok(jsonrpc_response_success(self.id, json!(None::<()>)))
        }
    }
}

#[derive(Debug)]
struct RpcRequestClusterNodes {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    id: Id<'static>,
}

impl RpcRequestHandler for RpcRequestClusterNodes {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        let request = no_params_expected(request)?;
        Ok(Self {
            state,
            x_subscription_id,
            id: request.id.into_owned(),
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        let Some(upstream) = self.state.upstream.as_ref() else {
            unreachable!();
        };

        upstream
            .get_cluster_nodes(self.x_subscription_id, deadline, self.id)
            .await
    }
}

#[derive(Debug)]
struct RpcRequestFirstAvailableBlock {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
}

impl RpcRequestHandler for RpcRequestFirstAvailableBlock {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        let request = no_params_expected(request)?;
        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: request.id.into_owned(),
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        if let Some(upstream) = (!self.upstream_disabled)
            .then_some(self.state.upstream.as_ref())
            .flatten()
        {
            upstream
                .get_first_available_block(self.x_subscription_id, deadline, &self.id)
                .await
        } else {
            Ok(jsonrpc_response_success(
                self.id,
                json!(self.state.stored_slots.first_available_load()),
            ))
        }
    }
}

#[derive(Debug)]
struct RpcRequestInflationReward {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    commitment: CommitmentConfig,
    epoch: Epoch,
    addresses: Vec<Pubkey>,
}

impl RpcRequestHandler for RpcRequestInflationReward {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            address_strs: Vec<String>,
            #[serde(default)]
            config: Option<RpcEpochConfig>,
        }

        let (
            id,
            ReqParams {
                address_strs,
                config,
            },
        ) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
        let mut addresses = Vec::with_capacity(address_strs.len());
        for address_str in address_strs.iter() {
            match verify_pubkey(address_str) {
                Ok(pubkey) => addresses.push(pubkey),
                Err(error) => return Err(jsonrpc_response_error(id, error)),
            }
        }
        let RpcEpochConfig {
            epoch,
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default();
        let (id, epoch) = match epoch {
            Some(epoch) => (id, epoch),
            None => {
                let (id, slot) = min_context_check(id, min_context_slot, commitment, &state)?;
                let slot = slot.unwrap_or_else(|| match commitment.commitment {
                    CommitmentLevel::Processed => state.stored_slots.processed_load(),
                    CommitmentLevel::Confirmed => state.stored_slots.confirmed_load(),
                    CommitmentLevel::Finalized => state.stored_slots.finalized_load(),
                });
                (id, state.epoch_schedule.get_epoch(slot).saturating_sub(1))
            }
        };

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            commitment,
            epoch,
            addresses,
        })
    }

    async fn process(mut self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::InflationReward {
                    deadline,
                    epoch: self.epoch,
                    addresses: std::mem::take(&mut self.addresses),
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

        let rewards = match result {
            ReadResultInflationReward::Timeout => anyhow::bail!("timeout"),
            ReadResultInflationReward::Reward(ReadRequestResultInflationReward {
                addresses,
                mut rewards,
                missed,
                base,
            }) => {
                if !missed.is_empty()
                    && let Err(error) = self
                        .get_inflation_reward(deadline, addresses, &mut rewards, missed, base)
                        .await?
                {
                    return Ok(error);
                }
                rewards
            }
            ReadResultInflationReward::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // serialize
        Ok(jsonrpc_response_success(self.id, &rewards))
    }
}

impl RpcRequestInflationReward {
    async fn get_inflation_reward(
        &mut self,
        deadline: Instant,
        addresses: Vec<Pubkey>,
        rewards: &mut [Option<RpcInflationReward>],
        mut missed: Vec<usize>,
        base: Option<InflationRewardBaseValue>,
    ) -> anyhow::Result<Result<(), Vec<u8>>> {
        let epoch_boundary_block = match base {
            Some(base) => base,
            None => match self
                .get_inflation_reward_base(deadline, &addresses, rewards, &mut missed)
                .await?
            {
                Ok(base) => {
                    if missed.is_empty() {
                        return Ok(Ok(()));
                    }
                    base
                }
                Err(error) => return Ok(Err(error)),
            },
        };

        // append stake account rewards from partitions
        if let Some(num_partitions) = epoch_boundary_block.num_reward_partitions {
            let num_partitions = usize::try_from(num_partitions)
                .expect("num_partitions should never exceed usize::MAX");

            // fetch blocks with rewards
            let block_list = match self
                .get_blocks_with_limit(deadline, epoch_boundary_block.slot + 1, num_partitions)
                .await?
            {
                Ok(blocks) => blocks,
                Err(error) => return Ok(Err(error)),
            };

            // calculate partitions for addresses
            let hasher =
                EpochRewardsHasher::new(num_partitions, &epoch_boundary_block.previous_blockhash);
            let mut partition_index_addresses: HashMap<usize, Vec<usize>> = HashMap::default();
            for index in missed {
                let partition_index = hasher.clone().hash_address_to_partition(&addresses[index]);
                if partition_index < num_partitions {
                    partition_index_addresses
                        .entry(partition_index)
                        .or_insert_with(|| Vec::with_capacity(4))
                        .push(index);
                }
            }

            // fetch blocks with rewards
            for (partition_index, missed) in partition_index_addresses {
                let Some(slot) = block_list.get(partition_index).copied() else {
                    // If block_list.len() too short to contain
                    // partition_index, the epoch rewards period must be
                    // currently active.
                    let rewards_complete_block_height = epoch_boundary_block
                        .block_height
                        .map(|block_height| {
                            block_height
                                .saturating_add(num_partitions as u64)
                                .saturating_add(1)
                        })
                        .expect(
                            "every block after partitioned_epoch_reward_enabled should have a \
                                populated block_height",
                        );

                    return self
                        .error_epoch_rewards_period_active(deadline, rewards_complete_block_height)
                        .await
                        .map(Err);
                };

                // fetch block
                let block = match self.get_block_with_rewards(deadline, slot).await? {
                    Ok(block) => block,
                    Err(error) => return Ok(Err(error)),
                };

                // collect for addresses
                let parititon_reward_map =
                    self.filter_rewards(slot, block.rewards, &|reward_type| {
                        reward_type == RewardType::Staking
                    })?;
                for index in missed {
                    if let Some(reward) = parititon_reward_map.get(&addresses[index]) {
                        rewards[index] = Some(reward.clone());
                    }
                }

                // send to writer
                self.state.db_write_inflation_reward.push_partition(
                    self.epoch,
                    partition_index,
                    parititon_reward_map,
                );
            }
        }

        Ok(Ok(()))
    }

    async fn get_inflation_reward_base(
        &self,
        deadline: Instant,
        addresses: &[Pubkey],
        rewards: &mut [Option<RpcInflationReward>],
        missed: &mut Vec<usize>,
    ) -> anyhow::Result<Result<InflationRewardBaseValue, Vec<u8>>> {
        // get first slot in epoch
        let first_slot_in_epoch = self
            .state
            .epoch_schedule
            .get_first_slot_in_epoch(self.epoch.saturating_add(1));
        if first_slot_in_epoch > self.state.stored_slots.confirmed_load() {
            return Ok(Err(RpcRequestBlock::error_not_available(
                self.id.clone(),
                first_slot_in_epoch,
            )));
        }
        let first_confirmed_block_in_epoch = match self
            .get_blocks_with_limit(deadline, first_slot_in_epoch, 1)
            .await?
            .map(|vec| vec.first().copied())
        {
            Ok(Some(slot)) => slot,
            Ok(None) => {
                return Ok(Err(RpcRequestBlock::error_not_available(
                    self.id.clone(),
                    first_slot_in_epoch,
                )));
            }
            Err(error) => return Ok(Err(error)),
        };
        let epoch_boundary_block = match self
            .get_block_with_rewards(deadline, first_confirmed_block_in_epoch)
            .await?
        {
            Ok(block) => block,
            Err(error) => return Ok(Err(error)),
        };
        let previous_blockhash = Hash::from_str(&epoch_boundary_block.previous_blockhash)
            .expect("UiConfirmedBlock::previous_blockhash should be properly formed");

        // collect rewards from epoch boundary slot
        let epoch_has_partitioned_rewards = epoch_boundary_block.num_reward_partitions.is_some();
        let epoch_reward_map = self.filter_rewards(
            first_confirmed_block_in_epoch,
            epoch_boundary_block.rewards,
            &|reward_type| {
                reward_type == RewardType::Voting
                    || (!epoch_has_partitioned_rewards && reward_type == RewardType::Staking)
            },
        )?;
        missed.retain(|index| {
            if let Some(reward) = epoch_reward_map.get(&addresses[*index]) {
                rewards[*index] = Some(reward.clone());
                false
            } else {
                true
            }
        });

        // send to writer
        self.state.db_write_inflation_reward.push_base(
            self.epoch,
            first_confirmed_block_in_epoch,
            epoch_boundary_block.block_height,
            previous_blockhash,
            epoch_boundary_block.num_reward_partitions,
            epoch_reward_map,
        );

        Ok(Ok(InflationRewardBaseValue::new(
            first_confirmed_block_in_epoch,
            epoch_boundary_block.block_height,
            previous_blockhash,
            epoch_boundary_block.num_reward_partitions,
        )))
    }

    async fn get_blocks_with_limit(
        &self,
        deadline: Instant,
        start_slot: Slot,
        limit: usize,
    ) -> anyhow::Result<Result<Vec<Slot>, Vec<u8>>> {
        // some slot will be removed while we pass request, send to upstream
        let first_available_slot = self.state.stored_slots.first_available_load() + 150;
        if start_slot < first_available_slot
            && let Some(upstream) = (!self.upstream_disabled)
                .then_some(self.state.upstream.as_ref())
                .flatten()
        {
            return upstream
                .get_blocks_parsed(deadline, &self.id, start_slot, limit)
                .await
                .map_err(|error| anyhow::anyhow!(error));
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::Blocks {
                    deadline,
                    start_slot,
                    until: RpcRequestBlocksUntil::Limit(limit),
                    commitment: CommitmentConfig::confirmed(),
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
            ReadResultBlocks::Blocks(blocks) => Ok(Ok(blocks)),
            ReadResultBlocks::ReadError(error) => anyhow::bail!("read error: {error}"),
        }
    }

    async fn get_block_with_rewards(
        &self,
        deadline: Instant,
        slot: Slot,
    ) -> anyhow::Result<Result<UiConfirmedBlock, Vec<u8>>> {
        if slot <= self.state.stored_slots.first_available_load() {
            return self.get_block_with_rewards_upstream(deadline, slot).await;
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
                .requests_tx
                .send(ReadRequest::Block {
                    deadline,
                    slot,
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
                return self.get_block_with_rewards_upstream(deadline, slot).await;
            }
            ReadResultBlock::Dead => {
                return Ok(Err(RpcRequestBlock::error_skipped(self.id.clone(), slot)));
            }
            ReadResultBlock::NotAvailable => {
                return Ok(Err(RpcRequestBlock::error_not_available(
                    self.id.clone(),
                    slot,
                )));
            }
            ReadResultBlock::Block(bytes) => bytes,
            ReadResultBlock::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify that we still have data for that block (i.e. we read correct data)
        if slot <= self.state.stored_slots.first_available_load() {
            return self.get_block_with_rewards_upstream(deadline, slot).await;
        }

        // parse and encode
        RpcRequestBlockWorkRequest::parse_and_encode(
            &bytes,
            &self.id,
            slot,
            UiTransactionEncoding::Base58,
            BlockEncodingOptions {
                transaction_details: TransactionDetails::None,
                show_rewards: true,
                max_supported_transaction_version: None,
            },
        )
    }

    async fn get_block_with_rewards_upstream(
        &self,
        deadline: Instant,
        slot: Slot,
    ) -> anyhow::Result<Result<UiConfirmedBlock, Vec<u8>>> {
        if let Some(upstream) = (!self.upstream_disabled)
            .then_some(self.state.upstream.as_ref())
            .flatten()
        {
            match upstream.get_block_rewards(deadline, &self.id, slot).await {
                Ok(Ok(Some(block))) => Ok(Ok(block)),
                Ok(Ok(None)) => Ok(Err(RpcRequestBlock::error_not_available(
                    self.id.clone(),
                    slot,
                ))),
                Ok(Err(error)) => Ok(Err(error)),
                Err(error) => anyhow::bail!(error),
            }
        } else {
            Ok(Err(RpcRequestBlock::error_skipped_long_term_storage(
                self.id.clone(),
                slot,
            )))
        }
    }

    fn filter_rewards<F>(
        &self,
        slot: Slot,
        rewards: Option<Vec<Reward>>,
        reward_type_filter: &F,
    ) -> anyhow::Result<HashMap<Pubkey, RpcInflationReward>>
    where
        F: Fn(RewardType) -> bool,
    {
        rewards
            .into_iter()
            .flatten()
            .filter_map(move |reward| {
                reward.reward_type.is_some_and(reward_type_filter).then(|| {
                    let pubkey = reward
                        .pubkey
                        .parse()
                        .map_err(|_| anyhow::anyhow!("failed to parse {}", reward.pubkey))?;
                    let inflation_reward = RpcInflationReward {
                        epoch: self.epoch,
                        effective_slot: slot,
                        amount: reward.lamports.unsigned_abs(),
                        post_balance: reward.post_balance,
                        commission: reward.commission,
                    };
                    Ok((pubkey, inflation_reward))
                })
            })
            .collect()
    }

    async fn error_epoch_rewards_period_active(
        &self,
        deadline: Instant,
        rewards_complete_block_height: Slot,
    ) -> RpcRequestResult {
        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
        match result {
            ReadResultBlockHeight::Timeout => anyhow::bail!("timeout"),
            ReadResultBlockHeight::BlockHeight { block_height, slot } => {
                Ok(jsonrpc_response_error_custom(
                    self.id.clone(),
                    RpcCustomError::EpochRewardsPeriodActive {
                        slot,
                        current_block_height: block_height,
                        rewards_complete_block_height,
                    },
                ))
            }
            ReadResultBlockHeight::ReadError(error) => anyhow::bail!("read error: {error}"),
        }
    }
}

#[derive(Debug)]
struct RpcRequestLatestBlockhash {
    state: Arc<State>,
    id: Id<'static>,
    commitment: CommitmentConfig,
}

impl RpcRequestHandler for RpcRequestLatestBlockhash {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (id, ReqParams { config }) = parse_params(request)?;
        let RpcContextConfig {
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default();

        let (id, _slot) = min_context_check(id, min_context_slot, commitment, &state)?;

        Ok(Self {
            state,
            id: id.into_owned(),
            commitment,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
                let response = RpcResponse {
                    context: RpcResponseContext::new(slot),
                    value: RpcBlockhash {
                        blockhash,
                        last_valid_block_height,
                    },
                };
                Ok(jsonrpc_response_success(self.id, &response))
            }
            ReadResultLatestBlockhash::ReadError(error) => {
                anyhow::bail!("read error: {error}")
            }
        }
    }
}

#[derive(Debug)]
struct RpcRequestLeaderSchedule {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    slot: Slot,
    is_processed: bool,
    identity: Option<String>,
}

impl RpcRequestHandler for RpcRequestLeaderSchedule {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            options: Option<RpcLeaderScheduleConfigWrapper>,
            #[serde(default)]
            config: Option<RpcLeaderScheduleConfig>,
        }

        let (id, ReqParams { options, config }) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
        let (slot, maybe_config) = options.map(|options| options.unzip()).unwrap_or_default();
        let config = maybe_config.or(config).unwrap_or_default();

        if let Some(identity) = &config.identity
            && let Err(error) = verify_pubkey(identity)
        {
            return Err(jsonrpc_response_error(id, error));
        }

        let (slot, is_processed) = match slot {
            Some(slot) => {
                if slot > state.stored_slots.processed_load() {
                    return Err(jsonrpc_response_success(id, json!(None::<()>)));
                }
                (slot, slot > state.stored_slots.confirmed_load())
            }
            None => match config.commitment.unwrap_or_default().commitment {
                CommitmentLevel::Processed => (state.stored_slots.processed_load(), true),
                CommitmentLevel::Confirmed => (state.stored_slots.confirmed_load(), false),
                CommitmentLevel::Finalized => (state.stored_slots.finalized_load(), false),
            },
        };

        Ok(Self {
            state,
            x_subscription_id,
            id: id.into_owned(),
            slot,
            is_processed,
            identity: config.identity,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        let Some(upstream) = self.state.upstream.as_ref() else {
            unreachable!();
        };

        let epoch = self.state.epoch_schedule.get_epoch(self.slot);
        upstream
            .get_leader_schedule(
                self.x_subscription_id,
                deadline,
                self.id,
                epoch,
                self.slot,
                self.is_processed,
                self.identity,
            )
            .await
    }
}

#[derive(Debug)]
struct RpcRequestRecentPrioritizationFees {
    state: Arc<State>,
    id: Id<'static>,
    pubkeys: Vec<Pubkey>,
    percentile: Option<u16>,
}

impl RpcRequestHandler for RpcRequestRecentPrioritizationFees {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
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
        ) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };

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
            .map(|pubkey_str| verify_pubkey(&pubkey_str))
            .collect::<Result<Vec<_>, _>>()
        {
            Ok(pubkeys) => pubkeys,
            Err(error) => {
                return Err(jsonrpc_response_error(id, error));
            }
        };

        let percentile = if state.grpf_percentile {
            let RpcRecentPrioritizationFeesConfig { percentile } = config.unwrap_or_default();
            if let Some(percentile) = percentile
                && percentile > 10_000
            {
                return Err(jsonrpc_response_error(
                    id,
                    jsonrpc_error_invalid_params::<()>(
                        "Percentile is too big; max value is 10000",
                        None,
                    ),
                ));
            }
            percentile
        } else {
            None
        };

        Ok(Self {
            state,
            id: id.into_owned(),
            pubkeys,
            percentile,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
                Ok(jsonrpc_response_success(self.id, &fees))
            }
        }
    }
}

#[derive(Debug)]
struct RpcRequestSignaturesForAddress {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    commitment: CommitmentConfig,
    address: Pubkey,
    before: Option<Signature>,
    until: Option<Signature>,
    limit: usize,
}

impl RpcRequestHandler for RpcRequestSignaturesForAddress {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            address: String,
            #[serde(default)]
            config: Option<RpcSignaturesForAddressConfig>,
        }

        let (id, ReqParams { address, config }) = parse_params(request)?;
        let RpcSignaturesForAddressConfig {
            before,
            until,
            limit,
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();

        let (address, before, until, limit) = match verify_and_parse_signatures_for_address_params(
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
        if let Err(error) = check_is_at_least_confirmed(commitment) {
            return Err(jsonrpc_response_error(id, error));
        }

        let (id, _slot) = min_context_check(id, min_context_slot, commitment, &state)?;

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            commitment,
            address,
            before,
            until,
            limit,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
        let id = if !finished && !self.upstream_disabled && limit > 0 {
            if !signatures.is_empty() {
                before = signatures
                    .last()
                    .map(|sig| sig.signature.parse().expect("valid sig"));
            }

            match self.fetch_upstream(deadline, before, limit).await? {
                Ok((id, mut sigs)) => {
                    signatures.append(&mut sigs);
                    id
                }
                Err(error) => return Ok(error),
            }
        } else {
            self.id
        };

        Ok(jsonrpc_response_success(id, &signatures))
    }
}

impl RpcRequestSignaturesForAddress {
    async fn fetch_upstream(
        self,
        deadline: Instant,
        before: Option<Signature>,
        limit: usize,
    ) -> anyhow::Result<
        Result<(Id<'static>, Vec<RpcConfirmedTransactionStatusWithSignature>), Vec<u8>>,
    > {
        if let Some(upstream) = self.state.upstream.as_ref() {
            let bytes = upstream
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

            let result: Response<Vec<RpcConfirmedTransactionStatusWithSignature>> =
                serde_json::from_slice(&bytes)
                    .map_err(|_error| anyhow::anyhow!("failed to parse json from upstream"))?;

            let value = match result.payload {
                ResponsePayload::Success(value) => value,
                ResponsePayload::Error(_) => return Ok(Err(bytes.to_vec())),
            };

            Ok(Ok((self.id, value.into_owned())))
        } else {
            Ok(Ok((self.id, vec![])))
        }
    }
}

#[derive(Debug)]
struct RpcRequestSignatureStatuses {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    signatures: Vec<Signature>,
    search_transaction_history: bool,
}

impl RpcRequestHandler for RpcRequestSignatureStatuses {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            signature_strs: Vec<String>,
            #[serde(default)]
            config: Option<RpcSignatureStatusConfig>,
        }

        let (
            id,
            ReqParams {
                signature_strs,
                config,
            },
        ) = parse_params(request)?;

        if signature_strs.len() > MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS {
            let message =
                format!("Too many inputs provided; max {MAX_GET_SIGNATURE_STATUSES_QUERY_ITEMS}");
            return Err(jsonrpc_response_error(
                id,
                jsonrpc_error_invalid_params::<()>(message, None),
            ));
        }

        let mut signatures: Vec<Signature> = Vec::with_capacity(signature_strs.len());
        for signature_str in signature_strs {
            match verify_signature(&signature_str) {
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

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            signatures,
            search_transaction_history,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
            && !self.upstream_disabled
            && self.state.upstream.is_some()
            && statuses.iter().any(|status| status.is_none())
        {
            let mut signatures_history = Vec::new();
            for (signature, status) in self.signatures.iter().zip(statuses.iter()) {
                if status.is_none() {
                    signatures_history.push(signature);
                }
            }

            let mut signatures_upstream =
                match self.fetch_upstream(deadline, signatures_history).await? {
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

        let response = RpcResponse {
            context: RpcResponseContext::new(self.state.stored_slots.processed_load()),
            value: statuses,
        };
        Ok(jsonrpc_response_success(self.id, &response))
    }
}

impl RpcRequestSignatureStatuses {
    async fn fetch_upstream(
        &self,
        deadline: Instant,
        signatures: Vec<&Signature>,
    ) -> anyhow::Result<Result<Vec<Option<TransactionStatus>>, Vec<u8>>> {
        if let Some(upstream) = self.state.upstream.as_ref() {
            let bytes = upstream
                .get_signature_statuses(
                    Arc::clone(&self.x_subscription_id),
                    deadline,
                    &self.id,
                    signatures,
                )
                .await?;

            let result: Response<RpcResponse<Vec<Option<TransactionStatus>>>> =
                serde_json::from_slice(&bytes)
                    .map_err(|_error| anyhow::anyhow!("failed to parse json from upstream"))?;

            if let ResponsePayload::Error(_) = &result.payload {
                return Ok(Err(bytes.to_vec()));
            }

            let ResponsePayload::Success(value) = result.payload else {
                unreachable!();
            };
            Ok(Ok(value.into_owned().value))
        } else {
            Ok(Ok(vec![]))
        }
    }
}

#[derive(Debug)]
struct RpcRequestSlot;

impl RpcRequestHandler for RpcRequestSlot {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Default, Deserialize)]
        struct ReqParams {
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (id, ReqParams { config }) = if request.params.is_some() {
            parse_params(request)?
        } else {
            (request.id, Default::default())
        };
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

        if let Some(min_context_slot) = min_context_slot
            && context_slot < min_context_slot
        {
            return Err(jsonrpc_response_error_custom(
                id,
                RpcCustomError::MinContextSlotNotReached { context_slot },
            ));
        }

        Err(jsonrpc_response_success(id, context_slot))
    }
}

#[derive(Debug)]
struct RpcRequestTransaction {
    state: Arc<State>,
    x_subscription_id: Arc<str>,
    upstream_disabled: bool,
    id: Id<'static>,
    signature: Signature,
    commitment: CommitmentConfig,
    encoding: UiTransactionEncoding,
    max_supported_transaction_version: Option<u8>,
}

impl RpcRequestHandler for RpcRequestTransaction {
    fn parse(
        state: Arc<State>,
        x_subscription_id: Arc<str>,
        upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
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
        ) = parse_params(request)?;

        let signature = match verify_signature(&signature_str) {
            Ok(signature) => signature,
            Err(error) => return Err(jsonrpc_response_error(id, error)),
        };

        let config = config
            .map(|config| config.convert_to_current())
            .unwrap_or_default();
        let encoding = config.encoding.unwrap_or(UiTransactionEncoding::Json);
        let max_supported_transaction_version = config.max_supported_transaction_version;
        let commitment = config.commitment.unwrap_or_default();
        if let Err(error) = check_is_at_least_confirmed(commitment) {
            return Err(jsonrpc_response_error(id, error));
        }

        Ok(Self {
            state,
            x_subscription_id,
            upstream_disabled,
            id: id.into_owned(),
            signature,
            commitment,
            encoding,
            max_supported_transaction_version,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
                return self.fetch_upstream(deadline).await;
            }
            ReadResultTransaction::Transaction {
                slot,
                block_time,
                bytes,
            } => (slot, block_time, bytes),
            ReadResultTransaction::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify commitment
        if self.commitment.is_finalized() && self.state.stored_slots.finalized_load() < slot {
            return Ok(jsonrpc_response_success(self.id, json!(None::<()>)));
        }

        // verify that we still have data for that block (i.e. we read correct data)
        if slot <= self.state.stored_slots.first_available_load() {
            return self.fetch_upstream(deadline).await;
        }

        // parse, encode and serialize
        process_with_workers(RpcRequestTransactionWorkRequest::create(
            self, slot, block_time, bytes,
        ))
        .await
    }
}

impl RpcRequestTransaction {
    async fn fetch_upstream(self, deadline: Instant) -> RpcRequestResult {
        if let Some(upstream) = (!self.upstream_disabled)
            .then_some(self.state.upstream.as_ref())
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
    x_subscription_id: Arc<str>,
    id: Id<'static>,
    encoding: UiTransactionEncoding,
    max_supported_transaction_version: Option<u8>,
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
    ) -> (Arc<State>, WorkRequest, oneshot::Receiver<RpcRequestResult>) {
        let (tx, rx) = oneshot::channel();
        let this = Self {
            x_subscription_id: request.x_subscription_id,
            id: request.id,
            encoding: request.encoding,
            max_supported_transaction_version: request.max_supported_transaction_version,
            slot,
            block_time,
            bytes,
            tx: Some(tx),
        };
        (request.state, WorkRequest::Transaction(this), rx)
    }

    pub fn process(mut self) {
        if let Some(tx) = self.tx.take() {
            let ts = quanta::Instant::now();
            let _ = tx.send(Self::process2(
                self.bytes,
                self.slot,
                self.block_time,
                self.id,
                self.encoding,
                self.max_supported_transaction_version,
            ));
            gauge!(
                RPC_WORKERS_CPU_SECONDS_TOTAL,
                "x_subscription_id" => self.x_subscription_id,
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
        Ok(jsonrpc_response_success(id, &tx))
    }
}

#[derive(Debug)]
struct RpcRequestVersion;

impl RpcRequestHandler for RpcRequestVersion {
    fn parse(
        _state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        let request = no_params_expected(request)?;
        let version = solana_version::Version::default();
        Err(jsonrpc_response_success(
            request.id,
            json!(RpcVersionInfo {
                solana_core: version.to_string(),
                feature_set: Some(version.feature_set),
            }),
        ))
    }
}

#[derive(Debug)]
pub struct RpcRequestIsBlockhashValid {
    state: Arc<State>,
    id: Id<'static>,
    blockhash: String,
    commitment: CommitmentConfig,
}

impl RpcRequestHandler for RpcRequestIsBlockhashValid {
    fn parse(
        state: Arc<State>,
        _x_subscription_id: Arc<str>,
        _upstream_disabled: bool,
        request: Request<'_>,
    ) -> Result<Self, Vec<u8>> {
        #[derive(Debug, Deserialize)]
        struct ReqParams {
            blockhash: String,
            #[serde(default)]
            config: Option<RpcContextConfig>,
        }

        let (id, ReqParams { blockhash, config }) = parse_params(request)?;
        let RpcContextConfig {
            commitment,
            min_context_slot,
        } = config.unwrap_or_default();
        let commitment = commitment.unwrap_or_default();

        let (id, _slot) = min_context_check(id, min_context_slot, commitment, &state)?;

        if let Err(error) = Hash::from_str(&blockhash) {
            return Err(jsonrpc_response_error(
                id,
                jsonrpc_error_invalid_params::<()>(format!("{error:?}"), None),
            ));
        }

        Ok(Self {
            state,
            id: id.into_owned(),
            blockhash,
            commitment,
        })
    }

    async fn process(self) -> RpcRequestResult {
        let deadline = Instant::now() + self.state.request_timeout;

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.state
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
                let response = RpcResponse {
                    context: RpcResponseContext::new(slot),
                    value: is_valid,
                };
                Ok(jsonrpc_response_success(self.id, &response))
            }
            ReadResultBlockhashValid::ReadError(error) => {
                anyhow::bail!("read error: {error}")
            }
        }
    }
}
