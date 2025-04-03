use {
    crate::config::ConfigRpcUpstream,
    jsonrpsee_types::{Id, Response},
    reqwest::{Client, StatusCode, Version, header::CONTENT_TYPE},
    serde_json::json,
    solana_rpc_client_api::config::{
        RpcBlockConfig, RpcSignaturesForAddressConfig, RpcTransactionConfig,
    },
    solana_sdk::{
        clock::Slot, commitment_config::CommitmentConfig, pubkey::Pubkey, signature::Signature,
    },
    solana_transaction_status::{BlockEncodingOptions, UiTransactionEncoding},
    std::time::{Duration, Instant},
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

    pub async fn get_block(
        &self,
        deadline: Instant,
        id: &Id<'static>,
        slot: Slot,
        commitment: CommitmentConfig,
        encoding: UiTransactionEncoding,
        encoding_options: BlockEncodingOptions,
    ) -> RpcClientResult {
        self.call_with_timeout(
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

    pub async fn get_block_time(
        &self,
        deadline: Instant,
        id: &Id<'static>,
        slot: Slot,
    ) -> RpcClientResult {
        self.call_with_timeout(
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
        deadline: Instant,
        id: &Id<'static>,
        address: Pubkey,
        before: Option<Signature>,
        until: Option<Signature>,
        limit: usize,
        commitment: CommitmentConfig,
    ) -> RpcClientResult {
        self.call_with_timeout(
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

    pub async fn get_transaction(
        &self,
        deadline: Instant,
        id: &Id<'static>,
        signature: Signature,
        commitment: CommitmentConfig,
        encoding: UiTransactionEncoding,
        max_supported_transaction_version: Option<u8>,
    ) -> RpcClientResult {
        self.call_with_timeout(
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

    async fn call_with_timeout(&self, deadline: Instant, body: String) -> RpcClientResult {
        match timeout_at(deadline.into(), self.call(body)).await {
            Ok(result) => result,
            Err(_timeout) => anyhow::bail!("upstream timeout"),
        }
    }

    async fn call(&self, body: String) -> RpcClientResult {
        let mut retries = self.retries_max;
        let mut backoff = self.retries_backoff_init;
        loop {
            match self.call2(body.clone()).await {
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

    async fn call2(&self, body: String) -> RpcClientResult {
        let request = self
            .client
            .post(&self.endpoint)
            .version(self.version)
            .header(CONTENT_TYPE, "application/json")
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
