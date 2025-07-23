use {
    crate::{storage::files::StorageId, version::VERSION},
    human_size::Size,
    reqwest::Version,
    richat_client::grpc::ConfigGrpcClient,
    richat_shared::config::{ConfigTokio, deserialize_affinity, deserialize_num_str},
    rocksdb::DBCompressionType,
    serde::{
        Deserialize,
        de::{self, Deserializer},
    },
    solana_rpc_client_api::request::MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT,
    std::{
        collections::{HashMap, HashSet},
        fs::read_to_string as read_to_string_sync,
        net::{IpAddr, Ipv4Addr, SocketAddr},
        path::{Path, PathBuf},
        str::FromStr,
        time::Duration,
    },
};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub logs: ConfigLogs,
    #[serde(default)]
    pub metrics: ConfigMetrics,
    /// Rpc & Stream data sources
    #[serde(default)]
    pub source: ConfigSource,
    /// Storage
    pub storage: ConfigStorage,
    /// RPC
    pub rpc: ConfigRpc,
}

impl Config {
    pub fn load_from_file<P: AsRef<Path>>(file: P) -> anyhow::Result<Self> {
        let config = read_to_string_sync(&file)?;
        let mut config: Self = if matches!(
            file.as_ref().extension().and_then(|e| e.to_str()),
            Some("yml") | Some("yaml")
        ) {
            serde_yaml::from_str(&config)?
        } else {
            json5::from_str(&config)?
        };
        
        // Handle backward compatibility for upstream_jsonrpc
        config.ensure_upstream_compatibility()?;
        
        Ok(config)
    }
    
    /// Ensures backward compatibility by converting single upstream_jsonrpc to multiple format
    /// and validates the multiple upstream configuration
    fn ensure_upstream_compatibility(&mut self) -> anyhow::Result<()> {
        // If we have the old single upstream format but no multiple format, convert it
        if self.rpc.upstream_jsonrpc.is_some() && self.rpc.upstream_jsonrpc_multiple.is_none() {
            let old_config = self.rpc.upstream_jsonrpc.take().unwrap();
            self.rpc.upstream_jsonrpc_multiple = Some(ConfigRpcUpstreams {
                default: "default".to_string(),
                endpoints: HashMap::from([("default".to_string(), old_config)]),
                method_routing: HashMap::new(),
            });
        }
        
        // Validate the multiple upstream configuration if present
        if let Some(ref upstreams) = self.rpc.upstream_jsonrpc_multiple {
            self.validate_upstream_config(upstreams)?;
        }
        
        Ok(())
    }
    
    /// Validates the multiple upstream configuration
    fn validate_upstream_config(&self, upstreams: &ConfigRpcUpstreams) -> anyhow::Result<()> {
        // Ensure the default upstream exists in endpoints
        if !upstreams.endpoints.contains_key(&upstreams.default) {
            return Err(anyhow::anyhow!(
                "Default upstream '{}' not found in endpoints",
                upstreams.default
            ));
        }
        
        // Ensure all method routes point to existing upstream names
        for (method, upstream_name) in &upstreams.method_routing {
            if !upstreams.endpoints.contains_key(upstream_name) {
                return Err(anyhow::anyhow!(
                    "Method '{}' routes to non-existent upstream '{}'",
                    method,
                    upstream_name
                ));
            }
        }
        
        // Validate that all endpoint URLs are valid
        for (name, endpoint) in &upstreams.endpoints {
            if endpoint.endpoint.is_empty() {
                return Err(anyhow::anyhow!(
                    "Upstream '{}' has empty endpoint URL",
                    name
                ));
            }
            
            // Basic URL validation
            if !endpoint.endpoint.starts_with("http://") && !endpoint.endpoint.starts_with("https://") {
                return Err(anyhow::anyhow!(
                    "Upstream '{}' has invalid endpoint URL: {}",
                    name,
                    endpoint.endpoint
                ));
            }
        }
        
        Ok(())
    }
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigLogs {
    pub json: bool,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigMetrics {
    /// Endpoint of Prometheus service
    pub endpoint: SocketAddr,
}

impl Default for ConfigMetrics {
    fn default() -> Self {
        Self {
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 8001),
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSource {
    /// Tokio runtime: subscribe on new data, rpc requests, metrics server
    #[serde(default)]
    pub tokio: ConfigTokio,
    pub http: ConfigSourceHttp,
    pub stream: ConfigSourceStream,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSourceHttp {
    pub rpc: String,
    pub httpget: Option<String>,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub concurrency: usize,
}

impl Default for ConfigSourceHttp {
    fn default() -> Self {
        Self {
            rpc: "http://127.0.0.1:8899".to_owned(),
            httpget: None,
            timeout: Duration::from_secs(30),
            concurrency: 10,
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSourceStream {
    pub source: ConfigSourceStreamKind,
    #[serde(default)]
    pub reconnect: Option<ConfigSourceStreamReconnect>,
    #[serde(flatten)]
    pub config: ConfigGrpcClient,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "snake_case")]
pub enum ConfigSourceStreamKind {
    DragonsMouth,
    #[default]
    Richat,
}

#[derive(Debug, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSourceStreamReconnect {
    #[serde(with = "humantime_serde")]
    pub backoff_init: Duration,
    #[serde(with = "humantime_serde")]
    pub backoff_max: Duration,
}

impl Default for ConfigSourceStreamReconnect {
    fn default() -> Self {
        Self {
            backoff_init: Duration::from_millis(100),
            backoff_max: Duration::from_secs(1),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorage {
    /// Backfilling options
    pub backfilling: Option<ConfigStorageBackfilling>,
    /// Storage files for blocks
    pub blocks: ConfigStorageBlocks,
    /// Storage of slots and tx index (RocksDB)
    pub rocksdb: ConfigStorageRocksdb,
    /// Write thread config
    #[serde(default)]
    pub write: ConfigStorageWrite,
    /// Read threads options
    #[serde(default)]
    pub read: ConfigStorageRead,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorageBackfilling {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub sync_to: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorageBlocks {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
    #[serde(
        default = "ConfigStorageBlocks::default_http_getblock_max_retries",
        deserialize_with = "deserialize_num_str"
    )]
    pub http_getblock_max_retries: usize,
    #[serde(
        default = "ConfigStorageBlocks::default_http_getblock_backoff_init",
        with = "humantime_serde"
    )]
    pub http_getblock_backoff_init: Duration,
    pub files: Vec<ConfigStorageFile>,
}

impl ConfigStorageBlocks {
    const fn default_http_getblock_max_retries() -> usize {
        10
    }

    const fn default_http_getblock_backoff_init() -> Duration {
        Duration::from_millis(100)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorageFile {
    pub id: StorageId,
    pub path: PathBuf,
    #[serde(deserialize_with = "deserialize_humansize")]
    pub size: u64,
    #[serde(default = "ConfigStorageFile::default_new_blocks")]
    pub new_blocks: bool,
}

impl ConfigStorageFile {
    const fn default_new_blocks() -> bool {
        true
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorageRocksdb {
    pub path: PathBuf,
    #[serde(default)]
    pub index_slot_compression: ConfigStorageRocksdbCompression,
    #[serde(default)]
    pub index_sfa_compression: ConfigStorageRocksdbCompression,
    #[serde(
        default = "ConfigStorageRocksdb::default_read_workers",
        deserialize_with = "deserialize_num_str"
    )]
    pub read_workers: usize,
}

impl ConfigStorageRocksdb {
    fn default_read_workers() -> usize {
        num_cpus::get()
    }
}

#[derive(Debug, Default, Clone, Copy, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "lowercase")]
pub enum ConfigStorageRocksdbCompression {
    #[default]
    None,
    Snappy,
    Zlib,
    Bz2,
    Lz4,
    Lz4hc,
    Zstd,
}

impl From<ConfigStorageRocksdbCompression> for DBCompressionType {
    fn from(value: ConfigStorageRocksdbCompression) -> Self {
        match value {
            ConfigStorageRocksdbCompression::None => Self::None,
            ConfigStorageRocksdbCompression::Snappy => Self::Snappy,
            ConfigStorageRocksdbCompression::Zlib => Self::Zlib,
            ConfigStorageRocksdbCompression::Bz2 => Self::Bz2,
            ConfigStorageRocksdbCompression::Lz4 => Self::Lz4,
            ConfigStorageRocksdbCompression::Lz4hc => Self::Lz4hc,
            ConfigStorageRocksdbCompression::Zstd => Self::Zstd,
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigStorageWrite {
    // Thread affinity
    #[serde(deserialize_with = "deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigStorageRead {
    /// Number of threads
    pub threads: usize,
    /// Thread affinity
    #[serde(deserialize_with = "deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub thread_max_async_requests: usize,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub thread_max_files_requests: usize,
}

impl Default for ConfigStorageRead {
    fn default() -> Self {
        Self {
            threads: 1,
            affinity: None,
            thread_max_async_requests: 1024,
            thread_max_files_requests: 32,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigRpc {
    /// Endpoint of RPC service
    pub endpoint: SocketAddr,
    /// Tokio runtime for RPC
    #[serde(default)]
    pub tokio: ConfigTokio,
    #[serde(
        default = "ConfigRpc::default_body_limit",
        deserialize_with = "deserialize_humansize_usize"
    )]
    /// Max body size limit in bytes
    pub body_limit: usize,
    /// Request timeout
    #[serde(
        default = "ConfigRpc::default_request_timeout",
        with = "humantime_serde"
    )]
    pub request_timeout: Duration,
    /// Supported Http/Get methods
    #[serde(default)]
    pub calls_httpget: HashSet<ConfigRpcCallHttpGet>,
    /// Supported JSON-RPC calls
    #[serde(default)]
    pub calls_jsonrpc: HashSet<ConfigRpcCallJson>,
    /// Maximum number of Signatures in getSignaturesForAddress
    #[serde(
        default = "ConfigRpc::default_gsfa_limit",
        deserialize_with = "deserialize_num_str"
    )]
    pub gsfa_limit: usize,
    /// Enable transaction history for getSignatureStatuses
    #[serde(default = "ConfigRpc::default_gss_transaction_history")]
    pub gss_transaction_history: bool,
    /// Enable `percentile` in getRecentPrioritizationFees
    #[serde(default = "ConfigRpc::default_grpf_percentile")]
    pub grpf_percentile: bool,
    /// TTL of getClusterNodes
    #[serde(default = "ConfigRpc::default_gcn_cache_ttl", with = "humantime_serde")]
    pub gcn_cache_ttl: Duration,
    /// Max number of requests in the queue
    #[serde(
        default = "ConfigRpc::default_request_channel_capacity",
        deserialize_with = "deserialize_num_str"
    )]
    pub request_channel_capacity: usize,
    /// In case of removed data upstream would be used to fetch data
    #[serde(default)]
    pub upstream_httpget: Option<ConfigRpcUpstream>,
    /// In case of removed data upstream would be used to fetch data (legacy single upstream)
    #[serde(default)]
    pub upstream_jsonrpc: Option<ConfigRpcUpstream>,
    /// Multiple upstream JSON-RPC endpoints with method routing
    #[serde(default)]
    pub upstream_jsonrpc_multiple: Option<ConfigRpcUpstreams>,
    /// Thread pool to parse / encode data
    #[serde(default)]
    pub workers: ConfigRpcWorkers,
}

impl ConfigRpc {
    const fn default_body_limit() -> usize {
        50 * 1024 // 50KiB
    }

    const fn default_request_timeout() -> Duration {
        Duration::from_secs(60)
    }

    const fn default_gsfa_limit() -> usize {
        MAX_GET_CONFIRMED_SIGNATURES_FOR_ADDRESS2_LIMIT
    }

    const fn default_gss_transaction_history() -> bool {
        true
    }

    const fn default_grpf_percentile() -> bool {
        true
    }

    const fn default_gcn_cache_ttl() -> Duration {
        Duration::from_secs(1)
    }

    const fn default_request_channel_capacity() -> usize {
        4096
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum ConfigRpcCallHttpGet {
    GetBlock,
    GetTransaction,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum ConfigRpcCallJson {
    GetBlock,
    GetBlockHeight,
    GetBlocks,
    GetBlocksWithLimit,
    GetBlockTime,
    GetClusterNodes,
    GetFirstAvailableBlock,
    GetInflationReward,
    GetLatestBlockhash,
    GetLeaderSchedule,
    GetRecentPrioritizationFees,
    GetSignaturesForAddress,
    GetSignatureStatuses,
    GetSlot,
    GetTransaction,
    GetVersion,
    IsBlockhashValid,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigRpcUpstream {
    pub endpoint: String,
    pub user_agent: String,
    #[serde(deserialize_with = "ConfigRpcUpstream::deserialize_version")]
    pub version: Version,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
}

impl Default for ConfigRpcUpstream {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8899".to_owned(),
            user_agent: format!("alpamayo/v{}", VERSION.package),
            version: Version::default(),
            timeout: Duration::from_secs(30),
        }
    }
}

impl ConfigRpcUpstream {
    fn deserialize_version<'de, D>(deserializer: D) -> Result<Version, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(match String::deserialize(deserializer)?.as_str() {
            "HTTP/0.9" => Version::HTTP_09,
            "HTTP/1.0" => Version::HTTP_10,
            "HTTP/1.1" => Version::HTTP_11,
            "HTTP/2.0" => Version::HTTP_2,
            "HTTP/3.0" => Version::HTTP_3,
            value => {
                return Err(de::Error::custom(format!("unknown HTTP version: {value}")));
            }
        })
    }
}

/// Configuration for multiple upstream JSON-RPC endpoints with method-based routing
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigRpcUpstreams {
    /// Name of the default upstream endpoint to use for methods not explicitly routed
    pub default: String,
    /// Named upstream endpoints with their connection configurations
    pub endpoints: HashMap<String, ConfigRpcUpstream>,
    /// Method to upstream name mapping - methods not listed here use the default upstream
    #[serde(default)]
    pub method_routing: HashMap<String, String>,
}

impl ConfigRpcUpstreams {
    /// Get the upstream name for a given method, returning the default if not specifically routed
    pub fn get_upstream_for_method(&self, method: &str) -> &str {
        self.method_routing.get(method).map(|s| s.as_str()).unwrap_or(&self.default)
    }
    
    /// Get the upstream configuration for a given method
    pub fn get_upstream_config_for_method(&self, method: &str) -> Option<&ConfigRpcUpstream> {
        let upstream_name = self.get_upstream_for_method(method);
        self.endpoints.get(upstream_name)
    }
    
    /// Get the default upstream configuration
    pub fn get_default_upstream_config(&self) -> Option<&ConfigRpcUpstream> {
        self.endpoints.get(&self.default)
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigRpcWorkers {
    /// Number of worker threads
    #[serde(deserialize_with = "deserialize_num_str")]
    pub threads: usize,
    /// Threads affinity
    #[serde(deserialize_with = "deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
    /// Queue size
    #[serde(deserialize_with = "deserialize_num_str")]
    pub channel_size: usize,
}

impl Default for ConfigRpcWorkers {
    fn default() -> Self {
        Self {
            threads: num_cpus::get(),
            affinity: None,
            channel_size: 4096,
        }
    }
}

fn deserialize_humansize<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    let size: &str = Deserialize::deserialize(deserializer)?;

    Size::from_str(size)
        .map(|size| size.to_bytes())
        .map_err(|error| de::Error::custom(format!("failed to parse size {size:?}: {error}")))
}

fn deserialize_humansize_usize<'de, D>(deserializer: D) -> Result<usize, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_humansize(deserializer).map(|value| value as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn test_backward_compatibility_conversion() {
        let mut config = Config {
            logs: ConfigLogs::default(),
            metrics: ConfigMetrics::default(),
            source: ConfigSource::default(),
            storage: ConfigStorage {
                backfilling: None,
                blocks: ConfigStorageBlocks {
                    max: 1000,
                    http_getblock_max_retries: 10,
                    http_getblock_backoff_init: Duration::from_millis(100),
                    files: vec![],
                },
                rocksdb: ConfigStorageRocksdb {
                    path: PathBuf::from("./test"),
                    index_slot_compression: ConfigStorageRocksdbCompression::None,
                    index_sfa_compression: ConfigStorageRocksdbCompression::None,
                    read_workers: 1,
                },
                write: ConfigStorageWrite::default(),
                read: ConfigStorageRead::default(),
            },
            rpc: ConfigRpc {
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000),
                tokio: ConfigTokio::default(),
                body_limit: 50 * 1024,
                request_timeout: Duration::from_secs(60),
                calls_httpget: HashSet::new(),
                calls_jsonrpc: HashSet::new(),
                gsfa_limit: 1000,
                gss_transaction_history: true,
                grpf_percentile: true,
                gcn_cache_ttl: Duration::from_secs(1),
                request_channel_capacity: 4096,
                upstream_httpget: None,
                upstream_jsonrpc: Some(ConfigRpcUpstream {
                    endpoint: "http://127.0.0.1:8899".to_string(),
                    user_agent: "test".to_string(),
                    version: Version::HTTP_11,
                    timeout: Duration::from_secs(30),
                }),
                upstream_jsonrpc_multiple: None,
                workers: ConfigRpcWorkers::default(),
            },
        };

        // Test backward compatibility conversion
        config.ensure_upstream_compatibility().unwrap();

        // Should have converted single upstream to multiple format
        assert!(config.rpc.upstream_jsonrpc.is_none());
        assert!(config.rpc.upstream_jsonrpc_multiple.is_some());

        let upstreams = config.rpc.upstream_jsonrpc_multiple.unwrap();
        assert_eq!(upstreams.default, "default");
        assert_eq!(upstreams.endpoints.len(), 1);
        assert!(upstreams.endpoints.contains_key("default"));
        assert!(upstreams.method_routing.is_empty());
    }

    #[test]
    fn test_multiple_upstream_validation() {
        let mut config = Config {
            logs: ConfigLogs::default(),
            metrics: ConfigMetrics::default(),
            source: ConfigSource::default(),
            storage: ConfigStorage {
                backfilling: None,
                blocks: ConfigStorageBlocks {
                    max: 1000,
                    http_getblock_max_retries: 10,
                    http_getblock_backoff_init: Duration::from_millis(100),
                    files: vec![],
                },
                rocksdb: ConfigStorageRocksdb {
                    path: PathBuf::from("./test"),
                    index_slot_compression: ConfigStorageRocksdbCompression::None,
                    index_sfa_compression: ConfigStorageRocksdbCompression::None,
                    read_workers: 1,
                },
                write: ConfigStorageWrite::default(),
                read: ConfigStorageRead::default(),
            },
            rpc: ConfigRpc {
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000),
                tokio: ConfigTokio::default(),
                body_limit: 50 * 1024,
                request_timeout: Duration::from_secs(60),
                calls_httpget: HashSet::new(),
                calls_jsonrpc: HashSet::new(),
                gsfa_limit: 1000,
                gss_transaction_history: true,
                grpf_percentile: true,
                gcn_cache_ttl: Duration::from_secs(1),
                request_channel_capacity: 4096,
                upstream_httpget: None,
                upstream_jsonrpc: None,
                upstream_jsonrpc_multiple: Some(ConfigRpcUpstreams {
                    default: "main".to_string(),
                    endpoints: HashMap::from([
                        ("main".to_string(), ConfigRpcUpstream {
                            endpoint: "http://127.0.0.1:8899".to_string(),
                            user_agent: "test".to_string(),
                            version: Version::HTTP_11,
                            timeout: Duration::from_secs(30),
                        }),
                        ("archive".to_string(), ConfigRpcUpstream {
                            endpoint: "http://archive.example.com:8899".to_string(),
                            user_agent: "test".to_string(),
                            version: Version::HTTP_11,
                            timeout: Duration::from_secs(60),
                        }),
                    ]),
                    method_routing: HashMap::from([
                        ("getBlock".to_string(), "archive".to_string()),
                        ("getTransaction".to_string(), "archive".to_string()),
                    ]),
                }),
                workers: ConfigRpcWorkers::default(),
            },
        };

        // Should validate successfully
        config.ensure_upstream_compatibility().unwrap();
    }

    #[test]
    fn test_validation_missing_default_upstream() {
        let upstreams = ConfigRpcUpstreams {
            default: "nonexistent".to_string(),
            endpoints: HashMap::from([
                ("main".to_string(), ConfigRpcUpstream::default()),
            ]),
            method_routing: HashMap::new(),
        };

        let config = Config {
            logs: ConfigLogs::default(),
            metrics: ConfigMetrics::default(),
            source: ConfigSource::default(),
            storage: ConfigStorage {
                backfilling: None,
                blocks: ConfigStorageBlocks {
                    max: 1000,
                    http_getblock_max_retries: 10,
                    http_getblock_backoff_init: Duration::from_millis(100),
                    files: vec![],
                },
                rocksdb: ConfigStorageRocksdb {
                    path: PathBuf::from("./test"),
                    index_slot_compression: ConfigStorageRocksdbCompression::None,
                    index_sfa_compression: ConfigStorageRocksdbCompression::None,
                    read_workers: 1,
                },
                write: ConfigStorageWrite::default(),
                read: ConfigStorageRead::default(),
            },
            rpc: ConfigRpc {
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000),
                tokio: ConfigTokio::default(),
                body_limit: 50 * 1024,
                request_timeout: Duration::from_secs(60),
                calls_httpget: HashSet::new(),
                calls_jsonrpc: HashSet::new(),
                gsfa_limit: 1000,
                gss_transaction_history: true,
                grpf_percentile: true,
                gcn_cache_ttl: Duration::from_secs(1),
                request_channel_capacity: 4096,
                upstream_httpget: None,
                upstream_jsonrpc: None,
                upstream_jsonrpc_multiple: Some(upstreams.clone()),
                workers: ConfigRpcWorkers::default(),
            },
        };

        let result = config.validate_upstream_config(&upstreams);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("Default upstream 'nonexistent' not found"));
    }

    #[test]
    fn test_validation_invalid_method_routing() {
        let upstreams = ConfigRpcUpstreams {
            default: "main".to_string(),
            endpoints: HashMap::from([
                ("main".to_string(), ConfigRpcUpstream::default()),
            ]),
            method_routing: HashMap::from([
                ("getBlock".to_string(), "nonexistent".to_string()),
            ]),
        };

        let config = Config {
            logs: ConfigLogs::default(),
            metrics: ConfigMetrics::default(),
            source: ConfigSource::default(),
            storage: ConfigStorage {
                backfilling: None,
                blocks: ConfigStorageBlocks {
                    max: 1000,
                    http_getblock_max_retries: 10,
                    http_getblock_backoff_init: Duration::from_millis(100),
                    files: vec![],
                },
                rocksdb: ConfigStorageRocksdb {
                    path: PathBuf::from("./test"),
                    index_slot_compression: ConfigStorageRocksdbCompression::None,
                    index_sfa_compression: ConfigStorageRocksdbCompression::None,
                    read_workers: 1,
                },
                write: ConfigStorageWrite::default(),
                read: ConfigStorageRead::default(),
            },
            rpc: ConfigRpc {
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9000),
                tokio: ConfigTokio::default(),
                body_limit: 50 * 1024,
                request_timeout: Duration::from_secs(60),
                calls_httpget: HashSet::new(),
                calls_jsonrpc: HashSet::new(),
                gsfa_limit: 1000,
                gss_transaction_history: true,
                grpf_percentile: true,
                gcn_cache_ttl: Duration::from_secs(1),
                request_channel_capacity: 4096,
                upstream_httpget: None,
                upstream_jsonrpc: None,
                upstream_jsonrpc_multiple: Some(upstreams.clone()),
                workers: ConfigRpcWorkers::default(),
            },
        };

        let result = config.validate_upstream_config(&upstreams);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("routes to non-existent upstream"));
    }

    #[test]
    fn test_upstream_helper_methods() {
        let upstreams = ConfigRpcUpstreams {
            default: "main".to_string(),
            endpoints: HashMap::from([
                ("main".to_string(), ConfigRpcUpstream {
                    endpoint: "http://main.example.com:8899".to_string(),
                    user_agent: "test".to_string(),
                    version: Version::HTTP_11,
                    timeout: Duration::from_secs(30),
                }),
                ("archive".to_string(), ConfigRpcUpstream {
                    endpoint: "http://archive.example.com:8899".to_string(),
                    user_agent: "test".to_string(),
                    version: Version::HTTP_11,
                    timeout: Duration::from_secs(60),
                }),
            ]),
            method_routing: HashMap::from([
                ("getBlock".to_string(), "archive".to_string()),
                ("getTransaction".to_string(), "archive".to_string()),
            ]),
        };

        // Test get_upstream_for_method
        assert_eq!(upstreams.get_upstream_for_method("getBlock"), "archive");
        assert_eq!(upstreams.get_upstream_for_method("getTransaction"), "archive");
        assert_eq!(upstreams.get_upstream_for_method("getSlot"), "main"); // Uses default

        // Test get_upstream_config_for_method
        let block_config = upstreams.get_upstream_config_for_method("getBlock").unwrap();
        assert_eq!(block_config.endpoint, "http://archive.example.com:8899");
        assert_eq!(block_config.timeout, Duration::from_secs(60));

        let slot_config = upstreams.get_upstream_config_for_method("getSlot").unwrap();
        assert_eq!(slot_config.endpoint, "http://main.example.com:8899");
        assert_eq!(slot_config.timeout, Duration::from_secs(30));

        // Test get_default_upstream_config
        let default_config = upstreams.get_default_upstream_config().unwrap();
        assert_eq!(default_config.endpoint, "http://main.example.com:8899");
    }

    #[test]
    fn test_existing_config_backward_compatibility() {
        // Test that the existing config.yml format still works
        let yaml_content = r#"
logs:
  json: false
metrics:
  endpoint: 127.0.0.1:8001
source:
  http:
    rpc: http://127.0.0.1:8899/
    timeout: 30s
    concurrency: 10
  stream:
    source: richat
    endpoint: http://127.0.0.1:10000
storage:
  blocks:
    max: 1000
    files:
      - id: 0
        path: ./db/test
        size: 1gb
  rocksdb:
    path: ./db/rocksdb
rpc:
  endpoint: 127.0.0.1:9000
  body_limit: 50KiB
  request_timeout: 60s
  calls_jsonrpc:
    - getBlock
    - getTransaction
  upstream_jsonrpc:
    endpoint: http://127.0.0.1:8899
    user_agent: alpamayo/v0.1.0
    version: HTTP/1.1
    timeout: 30s
  workers:
    threads: 8
"#;

        let mut config: Config = serde_yaml::from_str(yaml_content).unwrap();
        
        // Should have the old format initially
        assert!(config.rpc.upstream_jsonrpc.is_some());
        assert!(config.rpc.upstream_jsonrpc_multiple.is_none());
        
        // After compatibility processing, should be converted
        config.ensure_upstream_compatibility().unwrap();
        
        assert!(config.rpc.upstream_jsonrpc.is_none());
        assert!(config.rpc.upstream_jsonrpc_multiple.is_some());
        
        let upstreams = config.rpc.upstream_jsonrpc_multiple.unwrap();
        assert_eq!(upstreams.default, "default");
        assert_eq!(upstreams.endpoints.len(), 1);
        assert!(upstreams.method_routing.is_empty());
    }

    #[test]
    fn test_new_multiple_upstream_config() {
        // Test the new multiple upstream configuration format
        let yaml_content = r#"
logs:
  json: false
metrics:
  endpoint: 127.0.0.1:8001
source:
  http:
    rpc: http://127.0.0.1:8899/
    timeout: 30s
    concurrency: 10
  stream:
    source: richat
    endpoint: http://127.0.0.1:10000
storage:
  blocks:
    max: 1000
    files:
      - id: 0
        path: ./db/test
        size: 1gb
  rocksdb:
    path: ./db/rocksdb
rpc:
  endpoint: 127.0.0.1:9000
  body_limit: 50KiB
  request_timeout: 60s
  calls_jsonrpc:
    - getBlock
    - getTransaction
  upstream_jsonrpc_multiple:
    default: "main"
    endpoints:
      main:
        endpoint: http://127.0.0.1:8899
        user_agent: alpamayo/v0.1.0
        version: HTTP/1.1
        timeout: 30s
      archive:
        endpoint: http://archive.example.com:8899
        user_agent: alpamayo/v0.1.0
        version: HTTP/1.1
        timeout: 60s
    method_routing:
      getBlock: "archive"
      getTransaction: "archive"
  workers:
    threads: 8
"#;

        let mut config: Config = serde_yaml::from_str(yaml_content).unwrap();
        
        // Should have the new format
        assert!(config.rpc.upstream_jsonrpc.is_none());
        assert!(config.rpc.upstream_jsonrpc_multiple.is_some());
        
        // Should validate successfully
        config.ensure_upstream_compatibility().unwrap();
        
        let upstreams = config.rpc.upstream_jsonrpc_multiple.unwrap();
        assert_eq!(upstreams.default, "main");
        assert_eq!(upstreams.endpoints.len(), 2);
        assert_eq!(upstreams.method_routing.len(), 2);
        assert_eq!(upstreams.get_upstream_for_method("getBlock"), "archive");
        assert_eq!(upstreams.get_upstream_for_method("getSlot"), "main");
    }
}
