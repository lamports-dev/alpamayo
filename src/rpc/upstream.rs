use {
    crate::{
        config::ConfigRpcUpstream,
        metrics::RPC_UPSTREAM_REQUESTS_TOTAL,
        rpc::{
            api::{X_ERROR, X_SLOT},
            api_jsonrpc::RpcRequestBlocksUntil,
        },
    },
    http_body_util::{BodyExt, Full as BodyFull},
    hyper::http::Result as HttpResult,
    jsonrpsee_types::{Id, Response},
    metrics::counter,
    reqwest::{Client, StatusCode, Version, header::CONTENT_TYPE},
    richat_shared::jsonrpc::helpers::{RpcResponse, X_SUBSCRIPTION_ID},
    serde_json::json,
    solana_rpc_client_api::config::{
        RpcBlockConfig, RpcSignatureStatusConfig, RpcSignaturesForAddressConfig,
        RpcTransactionConfig,
    },
    solana_sdk::{
        clock::Slot, commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature,
    },
    solana_transaction_status::{BlockEncodingOptions, UiTransactionEncoding},
    std::{sync::Arc, time::Instant},
    tokio::time::timeout_at,
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

#[derive(Debug)]
pub struct RpcClientJsonrpc {
    client: Client,
    endpoint: String,
    version: Version,
}

impl RpcClientJsonrpc {
    pub fn new(config: ConfigRpcUpstream) -> anyhow::Result<Self> {
        let client = Client::builder()
            .user_agent(config.user_agent)
            .timeout(config.timeout)
            .build()?;

        Ok(Self {
            client,
            endpoint: config.endpoint,
            version: config.version,
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

        self.call_with_timeout(
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

        self.call_with_timeout(
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

        self.call_with_timeout(
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

        self.call_with_timeout(
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

        self.call_with_timeout(
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

        self.call_with_timeout(
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
    }

    async fn call_with_timeout(
        &self,
        x_subscription_id: &str,
        body: String,
        deadline: Instant,
    ) -> RpcClientJsonrpcResult {
        match timeout_at(deadline.into(), self.call(x_subscription_id, body)).await {
            Ok(result) => result,
            Err(_timeout) => anyhow::bail!("upstream timeout"),
        }
    }

    async fn call(&self, x_subscription_id: &str, body: String) -> RpcClientJsonrpcResult {
        let request = self
            .client
            .post(&self.endpoint)
            .version(self.version)
            .header(CONTENT_TYPE, "application/json")
            .header("x-subscription-id", x_subscription_id)
            .body(body);

        let Ok(response) = request.send().await else {
            anyhow::bail!("request to upstream failed");
        };

        anyhow::ensure!(
            response.status() == StatusCode::OK,
            "upstream response with status code: {}",
            response.status()
        );

        let Ok(bytes) = response.bytes().await else {
            anyhow::bail!("failed to collect bytes from upstream");
        };

        serde_json::from_slice(&bytes)
            .map(|response: Response<'_, serde_json::Value>| response.into_owned())
            .map_err(|_error| anyhow::anyhow!("failed to parse json from upstream"))
    }
}
