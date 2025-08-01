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
        collections::HashSet,
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
        if matches!(
            file.as_ref().extension().and_then(|e| e.to_str()),
            Some("yml") | Some("yaml")
        ) {
            serde_yaml::from_str(&config).map_err(Into::into)
        } else {
            json5::from_str(&config).map_err(Into::into)
        }
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
    /// In case of removed data upstream would be used to fetch data
    #[serde(default)]
    pub upstream_jsonrpc: Option<ConfigRpcUpstream>,
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
