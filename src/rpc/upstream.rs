use {
    crate::{
        config::ConfigRpcUpstream,
        metrics::RPC_UPSTREAM_REQUESTS_TOTAL,
        rpc::{
            api::{X_ERROR, X_SLOT},
            api_jsonrpc::RpcRequestBlocksUntil,
        },
        util::HashMap,
    },
    futures::future::{BoxFuture, FutureExt, Shared},
    http_body_util::{BodyExt, Full as BodyFull},
    hyper::http::Result as HttpResult,
    jsonrpsee_types::{Id, Response, ResponsePayload},
    metrics::counter,
    quanta::Instant as QInstant,
    reqwest::{Client, StatusCode, Version, header::CONTENT_TYPE},
    richat_shared::jsonrpc::helpers::{RpcResponse, X_SUBSCRIPTION_ID, jsonrpc_response_success},
    serde_json::json,
    solana_rpc_client_api::config::{
        RpcBlockConfig, RpcLeaderScheduleConfig, RpcLeaderScheduleConfigWrapper,
        RpcSignatureStatusConfig, RpcSignaturesForAddressConfig, RpcTransactionConfig,
    },
    solana_sdk::{
        clock::Slot, commitment_config::CommitmentConfig, epoch_schedule::Epoch, pubkey::Pubkey,
        signature::Signature,
    },
    solana_transaction_status::{BlockEncodingOptions, UiTransactionEncoding},
    std::{
        borrow::Cow,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{sync::Mutex, time::timeout_at},
    url::Url,
};

#[derive(Debug)]
pub struct RpcClientHttpget {
    client: Client,
    url: Url,
    version: Version,
}

impl RpcClientHttpget {
    pub fn new(config: ConfigRpcUpstream) -> anyhow::Result<Self> {
        let client = Client::builder()
            .user_agent(config.user_agent)
            .timeout(config.timeout)
            .build()?;

        Ok(Self {
            client,
            url: config.endpoint.parse()?,
            version: config.version,
        })
    }

    pub async fn get_block(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        slot: Slot,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getBlock_rest",
        )
        .increment(1);

        let mut url = self.url.clone();
        let slot = slot.to_string();
        if let Ok(mut segments) = url.path_segments_mut() {
            segments.extend(&["block", &slot]);
        }

        self.call_with_timeout(url.as_str(), &x_subscription_id, deadline)
            .await
    }

    pub async fn get_transaction(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        signature: Signature,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getTransaction_rest",
        )
        .increment(1);

        let mut url = self.url.clone();
        let signature = signature.to_string();
        if let Ok(mut segments) = url.path_segments_mut() {
            segments.extend(&["tx", &signature]);
        }

        self.call_with_timeout(url.as_str(), &x_subscription_id, deadline)
            .await
    }

    async fn call_with_timeout(
        &self,
        url: &str,
        x_subscription_id: &str,
        deadline: Instant,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        match timeout_at(deadline.into(), self.call(url, x_subscription_id)).await {
            Ok(result) => result,
            Err(_timeout) => anyhow::bail!("upstream timeout"),
        }
    }

    async fn call(
        &self,
        url: &str,
        x_subscription_id: &str,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        let request = self
            .client
            .get(url)
            .version(self.version)
            .header(X_SUBSCRIPTION_ID, x_subscription_id);

        let Ok(response) = request.send().await else {
            anyhow::bail!("request to upstream failed");
        };

        let (status, x_slot, x_error) = if response.status() == StatusCode::BAD_REQUEST {
            (
                StatusCode::BAD_REQUEST,
                None,
                response.headers().get(X_ERROR).cloned(),
            )
        } else {
            (
                StatusCode::OK,
                response.headers().get(X_SLOT).cloned(),
                None,
            )
        };

        let Ok(bytes) = response.bytes().await else {
            anyhow::bail!("failed to collect bytes from upstream");
        };

        let mut response = hyper::Response::builder().status(status);
        if let Some(x_slot) = x_slot {
            response = response.header(X_SLOT, x_slot);
        }
        if let Some(x_error) = x_error {
            response = response.header(X_ERROR, x_error);
        }
        Ok(response.body(BodyFull::from(bytes).boxed()))
    }
}

type RpcClientJsonrpcResult = anyhow::Result<jsonrpsee_types::Response<'static, serde_json::Value>>;
type RpcClientJsonrpcResultRaw =
    Result<jsonrpsee_types::Response<'static, serde_json::Value>, Cow<'static, str>>;

#[derive(Debug)]
pub struct RpcClientJsonrpcInner {
    client: Client,
    endpoint: String,
    version: Version,
}

impl RpcClientJsonrpcInner {
    async fn call_with_timeout(
        &self,
        x_subscription_id: &str,
        body: String,
        deadline: Instant,
    ) -> RpcClientJsonrpcResultRaw {
        match timeout_at(deadline.into(), self.call(x_subscription_id, body)).await {
            Ok(result) => result,
            Err(_timeout) => Err(Cow::Borrowed("upstream timeout")),
        }
    }

    async fn call_get_success(
        &self,
        x_subscription_id: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<Cow<'_, serde_json::Value>, Cow<'static, str>> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "id": 0,
            "params": params
        })
        .to_string();

        match self.call(x_subscription_id, body).await?.payload {
            ResponsePayload::Success(value) => Ok(value),
            ResponsePayload::Error(error) => {
                Err(Cow::Owned(format!("failed to get value: {error:?}")))
            }
        }
    }

    async fn call(&self, x_subscription_id: &str, body: String) -> RpcClientJsonrpcResultRaw {
        let request = self
            .client
            .post(&self.endpoint)
            .version(self.version)
            .header(CONTENT_TYPE, "application/json")
            .header("x-subscription-id", x_subscription_id)
            .body(body);

        let Ok(response) = request.send().await else {
            return Err(Cow::Borrowed("request to upstream failed"));
        };

        if response.status() != StatusCode::OK {
            return Err(Cow::Owned(format!(
                "upstream response with status code: {}",
                response.status()
            )));
        }

        let Ok(bytes) = response.bytes().await else {
            return Err(Cow::Borrowed("failed to collect bytes from upstream"));
        };

        serde_json::from_slice(&bytes)
            .map(|response: Response<'_, serde_json::Value>| response.into_owned())
            .map_err(|_error| Cow::Borrowed("failed to parse json from upstream"))
    }
}

type CachedEpochSchedule =
    Shared<BoxFuture<'static, Result<Arc<serde_json::Value>, Cow<'static, str>>>>;

#[derive(Debug)]
pub struct RpcClientJsonrpc {
    inner: Arc<RpcClientJsonrpcInner>,
    cache: CachedRequests,
    cache_epoch_schedule: Arc<Mutex<HashMap<Epoch, CachedEpochSchedule>>>,
}

impl RpcClientJsonrpc {
    pub fn new(config: ConfigRpcUpstream, cache_ttl: Duration) -> anyhow::Result<Self> {
        let client = Client::builder()
            .user_agent(config.user_agent)
            .timeout(config.timeout)
            .build()?;

        Ok(Self {
            inner: Arc::new(RpcClientJsonrpcInner {
                client,
                endpoint: config.endpoint,
                version: config.version,
            }),
            cache: CachedRequests::new(cache_ttl),
            cache_epoch_schedule: Arc::default(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_block(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
        slot: Slot,
        commitment: CommitmentConfig,
        encoding: UiTransactionEncoding,
        encoding_options: BlockEncodingOptions,
    ) -> RpcClientJsonrpcResult {
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getBlock",
        )
        .increment(1);

        self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                json!({
                    "jsonrpc": "2.0",
                    "method": "getBlock",
                    "id": id,
                    "params": [slot, RpcBlockConfig {
                        encoding: Some(encoding),
                        transaction_details: Some(encoding_options.transaction_details),
                        rewards: Some(encoding_options.show_rewards),
                        commitment: Some(commitment),
                        max_supported_transaction_version: encoding_options
                            .max_supported_transaction_version,
                    }]
                })
                .to_string(),
                deadline,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error))
    }

    pub async fn get_blocks(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
        start_slot: Slot,
        until: RpcRequestBlocksUntil,
        commitment: CommitmentConfig,
    ) -> RpcClientJsonrpcResult {
        let method = match until {
            RpcRequestBlocksUntil::EndSlot(_) => "getBlocks",
            RpcRequestBlocksUntil::Limit(_) => "getBlocksWithLimit",
        };

        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => method,
        )
        .increment(1);

        self.inner.call_with_timeout(
            x_subscription_id.as_ref(),
            json!({
                "jsonrpc": "2.0",
                "method": method,
                "id": id,
                "params": match until {
                    RpcRequestBlocksUntil::EndSlot(end_slot) => json!([start_slot, end_slot, commitment]),
                    RpcRequestBlocksUntil::Limit(limit) => json!([start_slot, limit, commitment]),
                }
            })
            .to_string(),
            deadline,
        )
        .await
        .map_err(|error| anyhow::anyhow!(error))
    }

    pub async fn get_block_time(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
        slot: Slot,
    ) -> RpcClientJsonrpcResult {
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getBlockTime",
        )
        .increment(1);

        self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                json!({
                    "jsonrpc": "2.0",
                    "method": "getBlockTime",
                    "id": id,
                    "params": [slot]
                })
                .to_string(),
                deadline,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error))
    }

    pub async fn get_cluster_nodes(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: Id<'static>,
    ) -> RpcClientJsonrpcResult {
        let inner = Arc::clone(&self.inner);
        let payload = CachedRequest::get(&self.cache.get_cluster_nodes, deadline, move || {
            async move {
                let result = inner
                    .call_get_success(x_subscription_id.as_ref(), "getClusterNodes", json!([]))
                    .await
                    .map(|value| value.into_owned());
                counter!(
                    RPC_UPSTREAM_REQUESTS_TOTAL,
                    "x_subscription_id" => x_subscription_id,
                    "method" => "getClusterNodes",
                )
                .increment(1);
                result
            }
            .boxed()
        })
        .await
        .map_err(|error| anyhow::anyhow!(error))?;

        Ok(jsonrpc_response_success(id, payload))
    }

    pub async fn get_first_available_block(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
    ) -> RpcClientJsonrpcResult {
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getFirstAvailableBlock",
        )
        .increment(1);

        self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                json!({
                    "jsonrpc": "2.0",
                    "method": "getFirstAvailableBlock",
                    "id": id,
                    "params": []
                })
                .to_string(),
                deadline,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_leader_schedule(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: Id<'static>,
        epoch: Epoch,
        slot: Slot,
        is_processed: bool,
        identity: Option<String>,
    ) -> RpcClientJsonrpcResult {
        if is_processed {
            counter!(
                RPC_UPSTREAM_REQUESTS_TOTAL,
                "x_subscription_id" => Arc::clone(&x_subscription_id),
                "method" => "getLeaderSchedule",
            )
            .increment(1);

            return self
                .inner
                .call_with_timeout(
                    x_subscription_id.as_ref(),
                    json!({
                        "jsonrpc": "2.0",
                        "method": "getLeaderSchedule",
                        "id": id,
                        "params": [
                            RpcLeaderScheduleConfigWrapper::SlotOnly(Some(slot)),
                            RpcLeaderScheduleConfig { identity, commitment: Some(CommitmentConfig::processed()) }
                        ]
                    })
                    .to_string(),
                    deadline,
                )
                .await
                .map_err(|error| anyhow::anyhow!(error));
        }

        let mut locked = self.cache_epoch_schedule.lock().await;
        let request = locked
            .get(&epoch)
            .and_then(|fut| {
                if !fut.peek().map(|v| v.is_err()).unwrap_or_default() {
                    Some(fut.clone())
                } else {
                    None
                }
            })
            .unwrap_or_else(|| {
                let inner = Arc::clone(&self.inner);
                let fut = async move {
                    let result = inner
                        .call_get_success(
                            x_subscription_id.as_ref(),
                            "getLeaderSchedule",
                            json!([
                                RpcLeaderScheduleConfigWrapper::SlotOnly(Some(slot)),
                                RpcLeaderScheduleConfig {
                                    identity: None,
                                    commitment: Some(CommitmentConfig::confirmed())
                                }
                            ]),
                        )
                        .await
                        .map(|value| Arc::new(value.into_owned()));
                    counter!(
                        RPC_UPSTREAM_REQUESTS_TOTAL,
                        "x_subscription_id" => x_subscription_id,
                        "method" => "getLeaderSchedule",
                    )
                    .increment(1);
                    if let Ok(payload) = &result {
                        if !payload.is_null() && !payload.is_object() {
                            return Err(Cow::Borrowed("invalid response type"));
                        }
                    }
                    result
                }
                .boxed()
                .shared();
                locked.insert(epoch, fut.clone());
                fut
            });
        drop(locked);

        let payload = match timeout_at(deadline.into(), request).await {
            Ok(result) => result,
            Err(_timeout) => Err(Cow::Borrowed("upstream timeout")),
        }
        .map_err(|error| anyhow::anyhow!(error))?;

        if let Some(identity) = identity {
            if payload.is_null() {
                return Ok(jsonrpc_response_success(id, serde_json::Value::Null));
            }

            let Some(map) = payload.as_object() else {
                unreachable!()
            };

            if let Some(slots) = map.get(&identity) {
                Ok(jsonrpc_response_success(
                    id,
                    json!({ identity.to_string(): slots }),
                ))
            } else {
                Ok(jsonrpc_response_success(id, json!({})))
            }
        } else {
            Ok(jsonrpc_response_success(id, payload.as_ref().clone()))
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_signatures_for_address(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
        address: Pubkey,
        before: Option<Signature>,
        until: Option<Signature>,
        limit: usize,
        commitment: CommitmentConfig,
    ) -> RpcClientJsonrpcResult {
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getSignaturesForAddress",
        )
        .increment(1);

        self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                json!({
                    "jsonrpc": "2.0",
                    "method": "getSignaturesForAddress",
                    "id": id,
                    "params": [address.to_string(), RpcSignaturesForAddressConfig {
                        before: before.map(|s| s.to_string()),
                        until: until.map(|s| s.to_string()),
                        limit: Some(limit),
                        commitment: Some(commitment),
                        min_context_slot: None,
                    }]
                })
                .to_string(),
                deadline,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error))
    }

    pub async fn get_signature_statuses(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
        signatures: Vec<&Signature>,
    ) -> RpcClientJsonrpcResult {
        let signatures = signatures.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getSignatureStatuses",
        )
        .increment(1);

        self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                json!({
                    "jsonrpc": "2.0",
                    "method": "getSignatureStatuses",
                    "id": id,
                    "params": [signatures, RpcSignatureStatusConfig {
                        search_transaction_history: true
                    }]
                })
                .to_string(),
                deadline,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_transaction(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
        signature: Signature,
        commitment: CommitmentConfig,
        encoding: UiTransactionEncoding,
        max_supported_transaction_version: Option<u8>,
    ) -> RpcClientJsonrpcResult {
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getTransaction",
        )
        .increment(1);

        self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                json!({
                    "jsonrpc": "2.0",
                    "method": "getTransaction",
                    "id": id,
                    "params": [signature.to_string(), RpcTransactionConfig {
                        encoding: Some(encoding),
                        commitment: Some(commitment),
                        max_supported_transaction_version,
                    }]
                })
                .to_string(),
                deadline,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error))
    }
}

#[derive(Debug)]
struct CachedRequests {
    get_cluster_nodes: Mutex<CachedRequest<serde_json::Value>>,
}

impl CachedRequests {
    fn new(ttl: Duration) -> Self {
        Self {
            get_cluster_nodes: Mutex::new(CachedRequest::new(ttl)),
        }
    }
}

#[derive(Debug)]
struct CachedRequest<T> {
    ttl: Duration,
    ts: QInstant,
    request: Shared<BoxFuture<'static, Result<T, Cow<'static, str>>>>,
}

impl<T: Clone> CachedRequest<T> {
    fn new(ttl: Duration) -> Self {
        Self {
            ttl,
            ts: QInstant::now().checked_sub(ttl * 2).unwrap(),
            request: async move { Err(Cow::Borrowed("uninit")) }.boxed().shared(),
        }
    }

    async fn get(
        request: &Mutex<Self>,
        deadline: Instant,
        fetch: impl FnOnce() -> BoxFuture<'static, Result<T, Cow<'static, str>>>,
    ) -> Result<T, Cow<'static, str>> {
        let mut locked = request.lock().await;
        if locked.ts.elapsed() >= locked.ttl
            || locked
                .request
                .peek()
                .map(|v| v.is_err())
                .unwrap_or_default()
        {
            locked.ts = QInstant::now();
            locked.request = fetch().shared();
        };
        let request = locked.request.clone();
        drop(locked);

        match timeout_at(deadline.into(), request).await {
            Ok(result) => result,
            Err(_timeout) => Err(Cow::Borrowed("upstream timeout")),
        }
    }
}
