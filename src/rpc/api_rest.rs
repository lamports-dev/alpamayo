use {
    crate::{
        config::{ConfigRpc, ConfigRpcCallRest},
        metrics::RPC_REQUESTS_TOTAL,
        rpc::{
            api::{
                RpcResponse, X_SLOT, check_call_support, get_x_bigtable_disabled,
                get_x_subscription_id, response_200, response_400, response_500,
            },
            upstream::RpcClientRest,
        },
        storage::{
            read::{ReadRequest, ReadResultBlock, ReadResultTransaction},
            slots::StoredSlots,
        },
        version::VERSION,
    },
    futures::future::BoxFuture,
    http_body_util::{BodyExt, Full as BodyFull},
    hyper::{body::Incoming as BodyIncoming, http::Result as HttpResult},
    metrics::counter,
    regex::Regex,
    solana_sdk::{clock::Slot, signature::Signature},
    std::{
        str::FromStr,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::sync::{mpsc, oneshot},
};

#[derive(Debug)]
struct SupportedCalls {
    get_block: Option<Regex>,
    get_transaction: Option<Regex>,
}

impl SupportedCalls {
    fn new(calls: &[ConfigRpcCallRest]) -> anyhow::Result<Self> {
        Ok(Self {
            get_block: check_call_support(calls, ConfigRpcCallRest::GetBlock)?
                .then(|| Regex::new(r"^/block/(\d{1,9})/?$"))
                .transpose()?,
            get_transaction: check_call_support(calls, ConfigRpcCallRest::GetTransaction)?
                .then(|| Regex::new(r"^/tx/([1-9A-HJ-NP-Za-km-z]{64,88})/?$"))
                .transpose()?,
        })
    }
}

#[derive(Debug)]
pub struct State {
    stored_slots: StoredSlots,
    request_timeout: Duration,
    supported_calls: SupportedCalls,
    requests_tx: mpsc::Sender<ReadRequest>,
    upstream: Option<RpcClientRest>,
}

impl State {
    pub fn new(
        config: &ConfigRpc,
        stored_slots: StoredSlots,
        requests_tx: mpsc::Sender<ReadRequest>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            stored_slots,
            request_timeout: config.request_timeout,
            supported_calls: SupportedCalls::new(&config.calls_rest)?,
            requests_tx,
            upstream: config
                .upstream_rest
                .clone()
                .map(RpcClientRest::new)
                .transpose()?,
        })
    }

    pub fn get_handler(
        self: Arc<Self>,
        req: hyper::Request<BodyIncoming>,
    ) -> Option<BoxFuture<'static, HttpResult<RpcResponse>>> {
        let path = req.uri().path();

        if let Some(re) = &self.supported_calls.get_block {
            if let Some(slot) = re.captures(path).and_then(|c| c.get(1).map(|m| m.as_str())) {
                if let Ok(slot) = Slot::from_str(slot) {
                    return Some(Box::pin(async move {
                        match self.process_block(req, slot).await {
                            Ok(response) => response,
                            Err(error) => response_500(error),
                        }
                    }));
                }
            }
        }

        if let Some(re) = &self.supported_calls.get_transaction {
            if let Some(slot) = re.captures(path).and_then(|c| c.get(1).map(|m| m.as_str())) {
                if let Ok(signature) = Signature::from_str(slot) {
                    return Some(Box::pin(async move {
                        match self.process_transaction(req, signature).await {
                            Ok(response) => response,
                            Err(error) => response_500(error),
                        }
                    }));
                }
            }
        }

        if path == "/version" {
            return Some(Box::pin(async move {
                response_200(
                    serde_json::json!({
                        "version": VERSION.version,
                        "solana": VERSION.solana,
                        "git": VERSION.git,
                    })
                    .to_string(),
                )
            }));
        }

        None
    }

    async fn process_block(
        &self,
        req: hyper::Request<BodyIncoming>,
        slot: Slot,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        let deadline = Instant::now() + self.request_timeout;

        let x_subscription_id = get_x_subscription_id(req.headers());
        let upstream_disabled = get_x_bigtable_disabled(req.headers());

        counter!(
            RPC_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getBlock_rest",
        )
        .increment(1);

        // check slot before sending request
        let slot_tip = self.stored_slots.confirmed_load();
        if slot > slot_tip {
            return Self::block_error_not_available(slot);
        }
        if slot <= self.stored_slots.first_available_load() {
            return self
                .get_block_upstream(upstream_disabled, x_subscription_id, deadline, slot)
                .await;
        }

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.requests_tx
                .send(ReadRequest::Block {
                    deadline,
                    slot,
                    tx,
                    x_subscription_id: Arc::clone(&x_subscription_id),
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
                    .get_block_upstream(upstream_disabled, x_subscription_id, deadline, slot)
                    .await;
            }
            ReadResultBlock::Dead => {
                return Self::block_error_skipped(slot);
            }
            ReadResultBlock::NotAvailable => {
                return Self::block_error_not_available(slot);
            }
            ReadResultBlock::Block(bytes) => bytes,
            ReadResultBlock::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify that we still have data for that block (i.e. we read correct data)
        if slot <= self.stored_slots.first_available_load() {
            return self
                .get_block_upstream(upstream_disabled, x_subscription_id, deadline, slot)
                .await;
        }

        Ok(hyper::Response::builder().body(BodyFull::from(bytes).boxed()))
    }

    async fn get_block_upstream(
        &self,
        upstream_disabled: bool,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        slot: Slot,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        if let Some(upstream) = (!upstream_disabled)
            .then_some(self.upstream.as_ref())
            .flatten()
        {
            upstream.get_block(x_subscription_id, deadline, slot).await
        } else {
            Self::block_error_skipped_long_term_storage(slot)
        }
    }

    fn block_error_not_available(slot: Slot) -> anyhow::Result<HttpResult<RpcResponse>> {
        let msg = format!("Block not available for slot {slot}\n");
        Ok(response_400(msg, Some("BlockNotAvailable".into())))
    }

    fn block_error_skipped(slot: Slot) -> anyhow::Result<HttpResult<RpcResponse>> {
        let msg =
            format!("Slot {slot} was skipped, or missing due to ledger jump to recent snapshot\n");
        Ok(response_400(msg, Some("SlotSkipped".into())))
    }

    fn block_error_skipped_long_term_storage(
        slot: Slot,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        let msg = format!("Slot {slot} was skipped, or missing in long-term storage\n");
        Ok(response_400(msg, Some("LongTermStorageSlotSkipped".into())))
    }

    async fn process_transaction(
        self: Arc<Self>,
        req: hyper::Request<BodyIncoming>,
        signature: Signature,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        let deadline = Instant::now() + self.request_timeout;

        let x_subscription_id = get_x_subscription_id(req.headers());
        let upstream_disabled = get_x_bigtable_disabled(req.headers());

        counter!(
            RPC_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getTransaction_rest",
        )
        .increment(1);

        // request
        let (tx, rx) = oneshot::channel();
        anyhow::ensure!(
            self.requests_tx
                .send(ReadRequest::Transaction {
                    deadline,
                    signature,
                    tx,
                    x_subscription_id: Arc::clone(&x_subscription_id),
                })
                .await
                .is_ok(),
            "request channel is closed"
        );
        let Ok(result) = rx.await else {
            anyhow::bail!("rx channel is closed");
        };
        let (slot, bytes) = match result {
            ReadResultTransaction::Timeout => anyhow::bail!("timeout"),
            ReadResultTransaction::NotFound => {
                return self
                    .get_transaction_upstream(
                        upstream_disabled,
                        x_subscription_id,
                        deadline,
                        signature,
                    )
                    .await;
            }
            ReadResultTransaction::Transaction {
                slot,
                block_time: _,
                bytes,
            } => (slot, bytes),
            ReadResultTransaction::ReadError(error) => anyhow::bail!("read error: {error}"),
        };

        // verify that we still have data for that block (i.e. we read correct data)
        if slot <= self.stored_slots.first_available_load() {
            return self
                .get_transaction_upstream(upstream_disabled, x_subscription_id, deadline, signature)
                .await;
        }

        Ok(hyper::Response::builder()
            .header(X_SLOT, slot)
            .body(BodyFull::from(bytes).boxed()))
    }

    async fn get_transaction_upstream(
        &self,
        upstream_disabled: bool,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        signature: Signature,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        if let Some(upstream) = (!upstream_disabled)
            .then_some(self.upstream.as_ref())
            .flatten()
        {
            upstream
                .get_transaction(x_subscription_id, deadline, signature)
                .await
        } else {
            Self::transaction_error_history_not_available()
        }
    }

    fn transaction_error_history_not_available() -> anyhow::Result<HttpResult<RpcResponse>> {
        let msg = "Transaction history is not available from this node\n".to_owned();
        Ok(response_400(
            msg,
            Some("TransactionHistoryNotAvailable".into()),
        ))
    }
}
