use {
    crate::{
        config::ConfigRpcUpstream, metrics::RPC_UPSTREAM_REQUESTS_TOTAL,
        rpc::api_solana::RpcRequestBlocksUntil,
    },
    jsonrpsee_types::{Id, Response},
    metrics::{Counter, counter},
    reqwest::{Client, StatusCode, Version, header::CONTENT_TYPE},
    serde_json::json,
    solana_rpc_client_api::config::{
        RpcBlockConfig, RpcSignatureStatusConfig, RpcSignaturesForAddressConfig,
        RpcTransactionConfig,
    },
    solana_sdk::{
        clock::Slot, commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature,
    },
    solana_transaction_status::{BlockEncodingOptions, UiTransactionEncoding},
    std::{
        sync::Arc,
        time::{Duration, Instant},
    },
    tokio::time::{sleep, timeout_at},
};

type RpcClientResult = anyhow::Result<jsonrpsee_types::Response<'static, serde_json::Value>>;

#[derive(Debug)]
pub struct RpcClient {
    client: Client,
    endpoint: String,
    version: Version,
    retries_max: usize,
    retries_backoff_init: Duration,
}

impl RpcClient {
    pub fn new(config: ConfigRpcUpstream) -> anyhow::Result<Self> {
        let client = Client::builder().user_agent(config.user_agent).build()?;

        Ok(Self {
            client,
            endpoint: config.endpoint,
            version: config.version,
            retries_max: config.retries_max,
            retries_backoff_init: config.retries_backoff_init,
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
    ) -> RpcClientResult {
        self.call_with_timeout(
            counter!(
                RPC_UPSTREAM_REQUESTS_TOTAL,
                "x_subscription_id" => Arc::clone(&x_subscription_id),
                "method" => "getBlock",
            ),
            x_subscription_id.as_ref(),
            deadline,
            serde_json::to_string(&json!({
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
            }))
            .expect("json serialization never fail"),
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
    ) -> RpcClientResult {
        let method = match until {
            RpcRequestBlocksUntil::EndSlot(_) => "getBlocks",
            RpcRequestBlocksUntil::Limit(_) => "getBlocksWithLimit",
        };

        self.call_with_timeout(
            counter!(
                RPC_UPSTREAM_REQUESTS_TOTAL,
                "x_subscription_id" => Arc::clone(&x_subscription_id),
                "method" => method,
            ),
            x_subscription_id.as_ref(),
            deadline,
            serde_json::to_string(&json!({
                "jsonrpc": "2.0",
                "method": method,
                "id": id,
                "params": match until {
                    RpcRequestBlocksUntil::EndSlot(end_slot) => json!([start_slot, end_slot, commitment]),
                    RpcRequestBlocksUntil::Limit(limit) => json!([start_slot, limit, commitment]),
                }
            }))
            .expect("json serialization never fail"),
        )
        .await
    }

    pub async fn get_block_time(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
        slot: Slot,
    ) -> RpcClientResult {
        self.call_with_timeout(
            counter!(
                RPC_UPSTREAM_REQUESTS_TOTAL,
                "x_subscription_id" => Arc::clone(&x_subscription_id),
                "method" => "getBlockTime",
            ),
            x_subscription_id.as_ref(),
            deadline,
            serde_json::to_string(&json!({
                "jsonrpc": "2.0",
                "method": "getBlockTime",
                "id": id,
                "params": [slot]
            }))
            .expect("json serialization never fail"),
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
    ) -> RpcClientResult {
        self.call_with_timeout(
            counter!(
                RPC_UPSTREAM_REQUESTS_TOTAL,
                "x_subscription_id" => Arc::clone(&x_subscription_id),
                "method" => "getSignaturesForAddress",
            ),
            x_subscription_id.as_ref(),
            deadline,
            serde_json::to_string(&json!({
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
            }))
            .expect("json serialization never fail"),
        )
        .await
    }

    pub async fn get_signature_statuses(
        &self,
        x_subscription_id: Arc<str>,
        deadline: Instant,
        id: &Id<'static>,
        signatures: Vec<&Signature>,
    ) -> RpcClientResult {
        let signatures = signatures.iter().map(|s| s.to_string()).collect::<Vec<_>>();

        self.call_with_timeout(
            counter!(
                RPC_UPSTREAM_REQUESTS_TOTAL,
                "x_subscription_id" => Arc::clone(&x_subscription_id),
                "method" => "getSignatureStatuses",
            ),
            x_subscription_id.as_ref(),
            deadline,
            serde_json::to_string(&json!({
                "jsonrpc": "2.0",
                "method": "getSignatureStatuses",
                "id": id,
                "params": [signatures, RpcSignatureStatusConfig {
                    search_transaction_history: true
                }]
            }))
            .expect("json serialization never fail"),
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
    ) -> RpcClientResult {
        self.call_with_timeout(
            counter!(
                RPC_UPSTREAM_REQUESTS_TOTAL,
                "x_subscription_id" => Arc::clone(&x_subscription_id),
                "method" => "getTransaction",
            ),
            x_subscription_id.as_ref(),
            deadline,
            serde_json::to_string(&json!({
                "jsonrpc": "2.0",
                "method": "getTransaction",
                "id": id,
                "params": [signature.to_string(), RpcTransactionConfig {
                    encoding: Some(encoding),
                    commitment: Some(commitment),
                    max_supported_transaction_version,
                }]
            }))
            .expect("json serialization never fail"),
        )
        .await
    }

    async fn call_with_timeout(
        &self,
        call_counter: Counter,
        x_subscription_id: &str,
        deadline: Instant,
        body: String,
    ) -> RpcClientResult {
        match timeout_at(
            deadline.into(),
            self.call(call_counter, x_subscription_id, body),
        )
        .await
        {
            Ok(result) => result,
            Err(_timeout) => anyhow::bail!("upstream timeout"),
        }
    }

    async fn call(
        &self,
        call_counter: Counter,
        x_subscription_id: &str,
        body: String,
    ) -> RpcClientResult {
        let mut retries = self.retries_max;
        let mut backoff = self.retries_backoff_init;
        loop {
            call_counter.increment(1);
            match self.call2(x_subscription_id, body.clone()).await {
                Ok(result) => return Ok(result),
                Err(error) => {
                    if retries == 0 {
                        return Err(error);
                    }

                    sleep(backoff).await;
                    retries -= 1;
                    backoff *= 2;
                }
            }
        }
    }

    async fn call2(&self, x_subscription_id: &str, body: String) -> RpcClientResult {
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
