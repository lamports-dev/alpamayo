use {
    crate::{
        config::{ConfigRpcUpstream, ConfigRpcUpstreams},
        metrics::{RPC_UPSTREAM_REQUESTS_TOTAL, RPC_UPSTREAM_REQUESTS_DURATION_SECONDS},
        rpc::{
            api::{X_ERROR, X_SLOT},
            api_jsonrpc::RpcRequestBlocksUntil,
        },
        util::HashMap,
    },
    futures::future::{BoxFuture, FutureExt, Shared},
    http_body_util::{BodyExt, Full as BodyFull},
    hyper::{body::Bytes, http::Result as HttpResult},
    jsonrpsee_types::{Id, Response, ResponsePayload},
    metrics::{counter, histogram},
    quanta::Instant as QInstant,
    reqwest::{Client, StatusCode, Version, header::CONTENT_TYPE},
    richat_shared::jsonrpc::helpers::{RpcResponse, X_SUBSCRIPTION_ID, jsonrpc_response_success},
    serde::de::DeserializeOwned,
    serde_json::json,
    solana_rpc_client_api::{
        config::{
            RpcBlockConfig, RpcLeaderScheduleConfig, RpcLeaderScheduleConfigWrapper,
            RpcSignatureStatusConfig, RpcSignaturesForAddressConfig, RpcTransactionConfig,
        },
        response::{RpcContactInfo, RpcLeaderSchedule},
    },
    solana_sdk::{
        clock::Slot, commitment_config::CommitmentConfig, epoch_schedule::Epoch, pubkey::Pubkey,
        signature::Signature,
    },
    solana_transaction_status::{BlockEncodingOptions, UiConfirmedBlock, UiTransactionEncoding},
    std::{
        borrow::Cow,
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::{sync::Mutex, time::timeout_at},
    tracing::error,
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
        upstream_name: &str,
        deadline: Instant,
        slot: Slot,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        let start_time = Instant::now();
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getBlock_rest",
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);

        let mut url = self.url.clone();
        let slot = slot.to_string();
        if let Ok(mut segments) = url.path_segments_mut() {
            segments.extend(&["block", &slot]);
        }

        let result = self.call_with_timeout(url.as_str(), &x_subscription_id, upstream_name, deadline)
            .await;
        
        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getBlock_rest",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());
        
        result
    }

    pub async fn get_transaction(
        &self,
        x_subscription_id: Arc<str>,
        upstream_name: &str,
        deadline: Instant,
        signature: Signature,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        let start_time = Instant::now();
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getTransaction_rest",
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);

        let mut url = self.url.clone();
        let signature = signature.to_string();
        if let Ok(mut segments) = url.path_segments_mut() {
            segments.extend(&["tx", &signature]);
        }

        let result = self.call_with_timeout(url.as_str(), &x_subscription_id, upstream_name, deadline)
            .await;
        
        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getTransaction_rest",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());
        
        result
    }

    async fn call_with_timeout(
        &self,
        url: &str,
        x_subscription_id: &str,
        upstream_name: &str,
        deadline: Instant,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        match timeout_at(deadline.into(), self.call(url, x_subscription_id, upstream_name)).await {
            Ok(result) => result,
            Err(_timeout) => {
                error!(
                    upstream = upstream_name,
                    url = %url,
                    x_subscription_id = %x_subscription_id,
                    "Upstream request timed out"
                );
                anyhow::bail!("upstream {} timeout", upstream_name)
            },
        }
    }

    async fn call(
        &self,
        url: &str,
        x_subscription_id: &str,
        upstream_name: &str,
    ) -> anyhow::Result<HttpResult<RpcResponse>> {
        let request = self
            .client
            .get(url)
            .version(self.version)
            .header(X_SUBSCRIPTION_ID, x_subscription_id);

        let Ok(response) = request.send().await else {
            error!(
                upstream = upstream_name,
                url = %url,
                x_subscription_id = %x_subscription_id,
                "HTTP request to upstream failed"
            );
            anyhow::bail!("request to upstream {} failed", upstream_name);
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
            error!(
                upstream = upstream_name,
                url = %url,
                x_subscription_id = %x_subscription_id,
                status = %status,
                "Failed to collect response bytes from upstream"
            );
            anyhow::bail!("failed to collect bytes from upstream {}", upstream_name);
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

type RpcClientJsonrpcResult = anyhow::Result<Vec<u8>>;
type RpcClientJsonrpcResultRaw = Result<Bytes, Cow<'static, str>>;

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
        upstream_name: &str,
        body: String,
        deadline: Instant,
    ) -> RpcClientJsonrpcResultRaw {
        match timeout_at(deadline.into(), self.call(x_subscription_id, upstream_name, body)).await {
            Ok(result) => result,
            Err(_timeout) => {
                error!(
                    upstream = upstream_name,
                    endpoint = %self.endpoint,
                    x_subscription_id = %x_subscription_id,
                    "JSON-RPC request to upstream timed out"
                );
                Err(Cow::Owned(format!("upstream {} timeout", upstream_name)))
            },
        }
    }

    async fn call_get_success<T: Clone + DeserializeOwned>(
        &self,
        x_subscription_id: &str,
        upstream_name: &str,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, Cow<'static, str>> {
        let body = json!({
            "jsonrpc": "2.0",
            "method": method,
            "id": 0,
            "params": params
        })
        .to_string();

        let bytes = self.call(x_subscription_id, upstream_name, body).await?;

        let result: Response<T> = serde_json::from_slice(&bytes)
            .map_err(|_error| Cow::Borrowed("failed to parse json from upstream"))?;

        match result.payload {
            ResponsePayload::Success(value) => Ok(value.into_owned()),
            ResponsePayload::Error(error) => {
                Err(Cow::Owned(format!("failed to get value: {error:?}")))
            }
        }
    }

    async fn call(&self, x_subscription_id: &str, upstream_name: &str, body: String) -> RpcClientJsonrpcResultRaw {
        let request = self
            .client
            .post(&self.endpoint)
            .version(self.version)
            .header(CONTENT_TYPE, "application/json")
            .header("x-subscription-id", x_subscription_id)
            .body(body);

        let Ok(response) = request.send().await else {
            error!(
                upstream = upstream_name,
                endpoint = %self.endpoint,
                x_subscription_id = %x_subscription_id,
                "JSON-RPC request to upstream failed"
            );
            return Err(Cow::Owned(format!("request to upstream {} failed", upstream_name)));
        };

        if response.status() != StatusCode::OK {
            error!(
                upstream = upstream_name,
                endpoint = %self.endpoint,
                x_subscription_id = %x_subscription_id,
                status = %response.status(),
                "Upstream returned non-OK status code"
            );
            return Err(Cow::Owned(format!(
                "upstream {} response with status code: {}",
                upstream_name,
                response.status()
            )));
        }

        response
            .bytes()
            .await
            .map_err(|error| {
                error!(
                    upstream = upstream_name,
                    endpoint = %self.endpoint,
                    x_subscription_id = %x_subscription_id,
                    error = %error,
                    "Failed to collect response bytes from upstream"
                );
                Cow::Owned(format!("failed to collect bytes from upstream {}", upstream_name))
            })
    }
}

type CachedEpochSchedule =
    Shared<BoxFuture<'static, Result<Arc<Option<RpcLeaderSchedule>>, Cow<'static, str>>>>;

#[derive(Debug)]
pub struct RpcClientJsonrpc {
    inner: Arc<RpcClientJsonrpcInner>,
    cache_cluster_nodes: CachedRequests<Vec<RpcContactInfo>>,
    cache_epoch_schedule: Arc<Mutex<HashMap<Epoch, CachedEpochSchedule>>>,
}

impl RpcClientJsonrpc {
    pub fn new(config: ConfigRpcUpstream, gcn_cache_ttl: Duration) -> anyhow::Result<Self> {
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
            cache_cluster_nodes: CachedRequests::new(gcn_cache_ttl),
            cache_epoch_schedule: Arc::default(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_block(
        &self,
        x_subscription_id: Arc<str>,
        upstream_name: &str,
        deadline: Instant,
        id: &Id<'static>,
        slot: Slot,
        commitment: CommitmentConfig,
        encoding: UiTransactionEncoding,
        encoding_options: BlockEncodingOptions,
    ) -> RpcClientJsonrpcResult {
        let start_time = Instant::now();
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getBlock",
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);

        let result = self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                upstream_name,
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
            .await;
        
        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getBlock",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());
        
        result
            .map(Into::into)
            .map_err(|error| anyhow::anyhow!(error))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_block_rewards(
        &self,
        upstream_name: &str,
        deadline: Instant,
        id: &Id<'static>,
        slot: Slot,
    ) -> anyhow::Result<Result<Option<UiConfirmedBlock>, Vec<u8>>> {
        let start_time = Instant::now();
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => "",
            "method" => "getBlock",
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);
        
        let bytes = self.inner
            .call_with_timeout(
                "",
                upstream_name,
                json!({
                    "jsonrpc": "2.0",
                    "method": "getBlock",
                    "id": id,
                    "params": [slot, RpcBlockConfig::rewards_with_commitment(Some(CommitmentConfig::confirmed()))]
                })
                .to_string(),
                deadline,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error))?;

        let result: Response<Option<UiConfirmedBlock>> = serde_json::from_slice(&bytes)
            .map_err(|_error| anyhow::anyhow!("failed to parse json from upstream"))?;

        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => "",
            "method" => "getBlock",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());

        let ResponsePayload::Success(value) = result.payload else {
            return Ok(Err(bytes.to_vec()));
        };

        Ok(Ok(value.into_owned()))
    }

    pub async fn get_blocks(
        &self,
        x_subscription_id: Arc<str>,
        upstream_name: &str,
        deadline: Instant,
        id: &Id<'static>,
        start_slot: Slot,
        until: RpcRequestBlocksUntil,
        commitment: CommitmentConfig,
    ) -> RpcClientJsonrpcResult {
        let start_time = Instant::now();
        let method = match until {
            RpcRequestBlocksUntil::EndSlot(_) => "getBlocks",
            RpcRequestBlocksUntil::Limit(_) => "getBlocksWithLimit",
        };

        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => method,
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);

        let result = self.inner.call_with_timeout(
            x_subscription_id.as_ref(),
            upstream_name,
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
        .await;
        
        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => method,
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());
        
        result
            .map(Into::into)
            .map_err(|error| anyhow::anyhow!(error))
    }

    pub async fn get_blocks_parsed(
        &self,
        upstream_name: &str,
        deadline: Instant,
        id: &Id<'static>,
        start_slot: Slot,
        limit: usize,
    ) -> anyhow::Result<Result<Vec<Slot>, Vec<u8>>> {
        let start_time = Instant::now();
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => "",
            "method" => "getBlocksWithLimit",
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);
        
        let bytes = self
            .inner
            .call_with_timeout(
                "",
                upstream_name,
                json!({
                    "jsonrpc": "2.0",
                    "method": "getBlocksWithLimit",
                    "id": id,
                    "params": json!([start_slot, limit, CommitmentConfig::confirmed()]),
                })
                .to_string(),
                deadline,
            )
            .await
            .map_err(|error| anyhow::anyhow!(error))?;

        let result: Response<Vec<Slot>> = serde_json::from_slice(&bytes)
            .map_err(|_error| anyhow::anyhow!("failed to parse json from upstream"))?;

        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => "",
            "method" => "getBlocksWithLimit",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());

        let ResponsePayload::Success(value) = result.payload else {
            return Ok(Err(bytes.to_vec()));
        };

        Ok(Ok(value.into_owned()))
    }

    pub async fn get_block_time(
        &self,
        x_subscription_id: Arc<str>,
        upstream_name: &str,
        deadline: Instant,
        id: &Id<'static>,
        slot: Slot,
    ) -> RpcClientJsonrpcResult {
        let start_time = Instant::now();
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getBlockTime",
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);

        let result = self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                upstream_name,
                json!({
                    "jsonrpc": "2.0",
                    "method": "getBlockTime",
                    "id": id,
                    "params": [slot]
                })
                .to_string(),
                deadline,
            )
            .await;
        
        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getBlockTime",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());
        
        result
            .map(Into::into)
            .map_err(|error| anyhow::anyhow!(error))
    }

    pub async fn get_cluster_nodes(
        &self,
        x_subscription_id: Arc<str>,
        upstream_name: &str,
        deadline: Instant,
        id: Id<'static>,
    ) -> RpcClientJsonrpcResult {
        let start_time = Instant::now();
        let inner = Arc::clone(&self.inner);
        let upstream_name_owned = upstream_name.to_string();
        let x_subscription_id_clone = Arc::clone(&x_subscription_id);
        let payload = self
            .cache_cluster_nodes
            .get(deadline, move || {
                async move {
                    let result = inner
                        .call_get_success::<Vec<RpcContactInfo>>(
                            x_subscription_id_clone.as_ref(),
                            &upstream_name_owned,
                            "getClusterNodes",
                            json!([]),
                        )
                        .await;
                    counter!(
                        RPC_UPSTREAM_REQUESTS_TOTAL,
                        "x_subscription_id" => x_subscription_id_clone,
                        "method" => "getClusterNodes",
                        "upstream" => upstream_name_owned.clone(),
                    )
                    .increment(1);
                    result
                }
                .boxed()
            })
            .await
            .map_err(|error| anyhow::anyhow!(error))?;

        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getClusterNodes",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());

        Ok(jsonrpc_response_success(id, payload))
    }

    pub async fn get_first_available_block(
        &self,
        x_subscription_id: Arc<str>,
        upstream_name: &str,
        deadline: Instant,
        id: &Id<'static>,
    ) -> RpcClientJsonrpcResult {
        let start_time = Instant::now();
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getFirstAvailableBlock",
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);

        let result = self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                upstream_name,
                json!({
                    "jsonrpc": "2.0",
                    "method": "getFirstAvailableBlock",
                    "id": id,
                    "params": []
                })
                .to_string(),
                deadline,
            )
            .await;
        
        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getFirstAvailableBlock",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());
        
        result
            .map(Into::into)
            .map_err(|error| anyhow::anyhow!(error))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_leader_schedule(
        &self,
        x_subscription_id: Arc<str>,
        upstream_name: &str,
        deadline: Instant,
        id: Id<'static>,
        epoch: Epoch,
        slot: Slot,
        is_processed: bool,
        identity: Option<String>,
    ) -> RpcClientJsonrpcResult {
        let start_time = Instant::now();
        if is_processed {
            counter!(
                RPC_UPSTREAM_REQUESTS_TOTAL,
                "x_subscription_id" => Arc::clone(&x_subscription_id),
                "method" => "getLeaderSchedule",
                "upstream" => upstream_name.to_string(),
            )
            .increment(1);

            let result = self
                .inner
                .call_with_timeout(
                    x_subscription_id.as_ref(),
                    upstream_name,
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
                .await;
            
            histogram!(
                RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
                "x_subscription_id" => Arc::clone(&x_subscription_id),
                "method" => "getLeaderSchedule",
                "upstream" => upstream_name.to_string(),
            )
            .record(start_time.elapsed().as_secs_f64());
            
            return result
                .map(Into::into)
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
                let upstream_name_owned = upstream_name.to_string();
                let x_subscription_id_clone = Arc::clone(&x_subscription_id);
                let fut = async move {
                    let result = inner
                        .call_get_success::<Option<RpcLeaderSchedule>>(
                            x_subscription_id_clone.as_ref(),
                            &upstream_name_owned,
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
                        .map(Arc::new);
                    counter!(
                        RPC_UPSTREAM_REQUESTS_TOTAL,
                        "x_subscription_id" => x_subscription_id_clone,
                        "method" => "getLeaderSchedule",
                        "upstream" => upstream_name_owned.clone(),
                    )
                    .increment(1);
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

        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getLeaderSchedule",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());

        if let Some(identity) = identity {
            let Some(map) = payload.as_ref() else {
                return Ok(jsonrpc_response_success(id, serde_json::Value::Null));
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
        upstream_name: &str,
        deadline: Instant,
        id: &Id<'static>,
        address: Pubkey,
        before: Option<Signature>,
        until: Option<Signature>,
        limit: usize,
        commitment: CommitmentConfig,
    ) -> RpcClientJsonrpcResult {
        let start_time = Instant::now();
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getSignaturesForAddress",
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);

        let result = self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                upstream_name,
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
            .await;
        
        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getSignaturesForAddress",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());
        
        result
            .map(Into::into)
            .map_err(|error| anyhow::anyhow!(error))
    }

    pub async fn get_signature_statuses(
        &self,
        x_subscription_id: Arc<str>,
        upstream_name: &str,
        deadline: Instant,
        id: &Id<'static>,
        signatures: Vec<&Signature>,
    ) -> RpcClientJsonrpcResult {
        let start_time = Instant::now();
        let signatures = signatures.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getSignatureStatuses",
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);

        let result = self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                upstream_name,
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
            .await;
        
        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getSignatureStatuses",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());
        
        result
            .map(Into::into)
            .map_err(|error| anyhow::anyhow!(error))
    }

    #[allow(clippy::too_many_arguments)]
    pub async fn get_transaction(
        &self,
        x_subscription_id: Arc<str>,
        upstream_name: &str,
        deadline: Instant,
        id: &Id<'static>,
        signature: Signature,
        commitment: CommitmentConfig,
        encoding: UiTransactionEncoding,
        max_supported_transaction_version: Option<u8>,
    ) -> RpcClientJsonrpcResult {
        let start_time = Instant::now();
        counter!(
            RPC_UPSTREAM_REQUESTS_TOTAL,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getTransaction",
            "upstream" => upstream_name.to_string(),
        )
        .increment(1);

        let result = self.inner
            .call_with_timeout(
                x_subscription_id.as_ref(),
                upstream_name,
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
            .await;
        
        histogram!(
            RPC_UPSTREAM_REQUESTS_DURATION_SECONDS,
            "x_subscription_id" => Arc::clone(&x_subscription_id),
            "method" => "getTransaction",
            "upstream" => upstream_name.to_string(),
        )
        .record(start_time.elapsed().as_secs_f64());
        
        result
            .map(Into::into)
            .map_err(|error| anyhow::anyhow!(error))
    }
}

#[derive(Debug)]
struct CachedRequests<T> {
    inner: Mutex<CachedRequest<T>>,
}

impl<T: Clone> CachedRequests<T> {
    fn new(ttl: Duration) -> Self {
        Self {
            inner: Mutex::new(CachedRequest::new(ttl)),
        }
    }

    async fn get(
        &self,
        deadline: Instant,
        fetch: impl FnOnce() -> BoxFuture<'static, Result<T, Cow<'static, str>>>,
    ) -> Result<T, Cow<'static, str>> {
        let mut locked = self.inner.lock().await;
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
}

/// Manager for multiple upstream JSON-RPC clients with method-based routing
#[derive(Debug)]
pub struct RpcClientJsonrpcManager {
    /// Name of the default upstream to use for methods not explicitly routed
    default_upstream: String,
    /// Map of upstream name to client instances
    clients: HashMap<String, RpcClientJsonrpc>,
    /// Map of method names to upstream names for routing
    method_routing: HashMap<String, String>,
}

impl RpcClientJsonrpcManager {
    /// Create a new manager from the multiple upstream configuration
    pub fn new(config: ConfigRpcUpstreams, gcn_cache_ttl: Duration) -> anyhow::Result<Self> {
        let mut clients = HashMap::default();
        
        // Create clients for each configured endpoint
        for (name, endpoint_config) in &config.endpoints {
            let client = RpcClientJsonrpc::new(endpoint_config.clone(), gcn_cache_ttl)?;
            clients.insert(name.clone(), client);
        }
        
        // Validate that the default upstream exists
        if !clients.contains_key(&config.default) {
            return Err(anyhow::anyhow!(
                "Default upstream '{}' not found in configured endpoints",
                config.default
            ));
        }
        
        // Validate that all method routes point to existing upstream names
        for (method, upstream_name) in &config.method_routing {
            if !clients.contains_key(upstream_name) {
                return Err(anyhow::anyhow!(
                    "Method '{}' routes to non-existent upstream '{}'",
                    method,
                    upstream_name
                ));
            }
        }
        
        // Convert the standard HashMap to the custom HashMap type
        let mut method_routing = HashMap::default();
        for (method, upstream) in config.method_routing {
            method_routing.insert(method, upstream);
        }
        
        Ok(Self {
            default_upstream: config.default,
            clients,
            method_routing,
        })
    }
    
    /// Get the appropriate client for a given method name
    /// Returns the client for the method-specific upstream, or the default upstream if no specific routing is configured
    pub fn get_client_for_method(&self, method: &str) -> Option<&RpcClientJsonrpc> {
        let upstream_name = self.method_routing
            .get(method)
            .map(|s| s.as_str())
            .unwrap_or(&self.default_upstream);
        
        self.clients.get(upstream_name)
    }
    
    /// Get the default upstream client
    pub fn get_default_client(&self) -> Option<&RpcClientJsonrpc> {
        self.clients.get(&self.default_upstream)
    }
    
    /// Get the upstream name that would be used for a given method
    pub fn get_upstream_name_for_method(&self, method: &str) -> &str {
        self.method_routing
            .get(method)
            .map(|s| s.as_str())
            .unwrap_or(&self.default_upstream)
    }
    
    /// Get all configured upstream names
    pub fn get_upstream_names(&self) -> impl Iterator<Item = &String> {
        self.clients.keys()
    }
    
    /// Get the number of configured upstream endpoints
    pub fn upstream_count(&self) -> usize {
        self.clients.len()
    }
    
    /// Check if a specific upstream exists
    pub fn has_upstream(&self, name: &str) -> bool {
        self.clients.contains_key(name)
    }
    
    /// Get a specific upstream client by name
    pub fn get_upstream_client(&self, name: &str) -> Option<&RpcClientJsonrpc> {
        self.clients.get(name)
    }
    
    /// Call get_block on the appropriate upstream for the method
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
        let upstream_name = self.get_upstream_name_for_method("getBlock");
        if let Some(client) = self.get_client_for_method("getBlock") {
            client.get_block(x_subscription_id, upstream_name, deadline, id, slot, commitment, encoding, encoding_options).await
        } else {
            anyhow::bail!("No upstream available for getBlock")
        }
    }
    
    /// Call get_blocks on the appropriate upstream for the method
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
        let upstream_name = self.get_upstream_name_for_method(method);
        if let Some(client) = self.get_client_for_method(method) {
            client.get_blocks(x_subscription_id, upstream_name, deadline, id, start_slot, until, commitment).await
        } else {
            anyhow::bail!("No upstream available for {}", method)
        }
    }
    
    /// Call get_block_time on the appropriate upstream for the method
    pub async fn get_block_time(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
        slot: Slot,
    ) -> RpcClientJsonrpcResult {
        let upstream_name = self.get_upstream_name_for_method("getBlockTime");
        if let Some(client) = self.get_client_for_method("getBlockTime") {
            client.get_block_time(x_subscription_id, upstream_name, deadline, id, slot).await
        } else {
            anyhow::bail!("No upstream available for getBlockTime")
        }
    }
    
    /// Call get_cluster_nodes on the appropriate upstream for the method
    pub async fn get_cluster_nodes(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: Id<'static>,
    ) -> RpcClientJsonrpcResult {
        let upstream_name = self.get_upstream_name_for_method("getClusterNodes");
        if let Some(client) = self.get_client_for_method("getClusterNodes") {
            client.get_cluster_nodes(x_subscription_id, upstream_name, deadline, id).await
        } else {
            anyhow::bail!("No upstream available for getClusterNodes")
        }
    }
    
    /// Call get_first_available_block on the appropriate upstream for the method
    pub async fn get_first_available_block(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
    ) -> RpcClientJsonrpcResult {
        let upstream_name = self.get_upstream_name_for_method("getFirstAvailableBlock");
        if let Some(client) = self.get_client_for_method("getFirstAvailableBlock") {
            client.get_first_available_block(x_subscription_id, upstream_name, deadline, id).await
        } else {
            anyhow::bail!("No upstream available for getFirstAvailableBlock")
        }
    }
    
    /// Call get_leader_schedule on the appropriate upstream for the method
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
        let upstream_name = self.get_upstream_name_for_method("getLeaderSchedule");
        if let Some(client) = self.get_client_for_method("getLeaderSchedule") {
            client.get_leader_schedule(x_subscription_id, upstream_name, deadline, id, epoch, slot, is_processed, identity).await
        } else {
            anyhow::bail!("No upstream available for getLeaderSchedule")
        }
    }
    
    /// Call get_signatures_for_address on the appropriate upstream for the method
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
        let upstream_name = self.get_upstream_name_for_method("getSignaturesForAddress");
        if let Some(client) = self.get_client_for_method("getSignaturesForAddress") {
            client.get_signatures_for_address(x_subscription_id, upstream_name, deadline, id, address, before, until, limit, commitment).await
        } else {
            anyhow::bail!("No upstream available for getSignaturesForAddress")
        }
    }
    
    /// Call get_signature_statuses on the appropriate upstream for the method
    pub async fn get_signature_statuses(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
        signatures: Vec<&Signature>,
    ) -> RpcClientJsonrpcResult {
        let upstream_name = self.get_upstream_name_for_method("getSignatureStatuses");
        if let Some(client) = self.get_client_for_method("getSignatureStatuses") {
            client.get_signature_statuses(x_subscription_id, upstream_name, deadline, id, signatures).await
        } else {
            anyhow::bail!("No upstream available for getSignatureStatuses")
        }
    }
    
    /// Call get_transaction on the appropriate upstream for the method
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
        let upstream_name = self.get_upstream_name_for_method("getTransaction");
        if let Some(client) = self.get_client_for_method("getTransaction") {
            client.get_transaction(x_subscription_id, upstream_name, deadline, id, signature, commitment, encoding, max_supported_transaction_version).await
        } else {
            anyhow::bail!("No upstream available for getTransaction")
        }
    }
    
    /// Call get_blocks_parsed on the appropriate upstream for the method
    pub async fn get_blocks_parsed(
        &self,
        deadline: Instant,
        id: &Id<'static>,
        start_slot: Slot,
        limit: usize,
    ) -> anyhow::Result<Result<Vec<Slot>, Vec<u8>>> {
        let upstream_name = self.get_upstream_name_for_method("getBlocksWithLimit");
        if let Some(client) = self.get_client_for_method("getBlocksWithLimit") {
            client.get_blocks_parsed(upstream_name, deadline, id, start_slot, limit).await
        } else {
            anyhow::bail!("No upstream available for getBlocksWithLimit")
        }
    }
    
    /// Call get_block_rewards on the appropriate upstream for the method
    pub async fn get_block_rewards(
        &self,
        deadline: Instant,
        id: &Id<'static>,
        slot: Slot,
    ) -> anyhow::Result<Result<Option<UiConfirmedBlock>, Vec<u8>>> {
        let upstream_name = self.get_upstream_name_for_method("getBlock");
        if let Some(client) = self.get_client_for_method("getBlock") {
            client.get_block_rewards(upstream_name, deadline, id, slot).await
        } else {
            anyhow::bail!("No upstream available for getBlock")
        }
    }
}
