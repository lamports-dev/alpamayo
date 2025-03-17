use {
    crate::{storage::files::StorageId, version::VERSION},
    human_size::Size,
    reqwest::Version,
    richat_client::grpc::ConfigGrpcClient,
    richat_shared::config::{ConfigTokio, deserialize_affinity, deserialize_num_str},
    serde::{
        Deserialize,
        de::{self, Deserializer},
    },
    std::{
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
    pub metrics: Option<ConfigMetrics>,
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
    pub rpc: ConfigSourceRpc,
    pub stream: ConfigSourceStream,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSourceRpc {
    pub url: String,
    #[serde(with = "humantime_serde")]
    pub timeout: Duration,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub concurrency: usize,
}

impl Default for ConfigSourceRpc {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8899".to_owned(),
            timeout: Duration::from_secs(30),
            concurrency: 10,
        }
    }
}

#[derive(Debug, Default, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigSourceStream {
    pub source: ConfigSourceStreamKind,
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

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorage {
    /// Thread affinity
    #[serde(default, deserialize_with = "deserialize_affinity")]
    pub affinity: Option<Vec<usize>>,
    #[serde(
        default = "ConfigStorage::default_read_requests_concurrency",
        deserialize_with = "deserialize_num_str"
    )]
    pub read_requests_concurrency: usize,
    pub blocks: ConfigStorageBlocks,
}

impl ConfigStorage {
    const fn default_read_requests_concurrency() -> usize {
        128
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorageBlocks {
    #[serde(deserialize_with = "deserialize_num_str")]
    pub max: usize,
    pub path: PathBuf,
    #[serde(
        default = "ConfigStorageBlocks::default_rpc_getblock_max_retries",
        deserialize_with = "deserialize_num_str"
    )]
    pub rpc_getblock_max_retries: usize,
    #[serde(
        default = "ConfigStorageBlocks::default_rpc_getblock_backoff_init",
        with = "humantime_serde"
    )]
    pub rpc_getblock_backoff_init: Duration,
    #[serde(
        default = "ConfigStorageBlocks::default_rpc_getblock_max_concurrency",
        deserialize_with = "deserialize_num_str"
    )]
    pub rpc_getblock_max_concurrency: usize,
    pub files: Vec<ConfigStorageFile>,
}

impl ConfigStorageBlocks {
    const fn default_rpc_getblock_max_retries() -> usize {
        10
    }

    const fn default_rpc_getblock_backoff_init() -> Duration {
        Duration::from_millis(100)
    }

    const fn default_rpc_getblock_max_concurrency() -> usize {
        15
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ConfigStorageFile {
    pub id: StorageId,
    pub path: PathBuf,
    #[serde(deserialize_with = "deserialize_humansize")]
    pub size: u64,
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
    /// Supported RPC calls
    pub calls: Vec<ConfigRpcCall>,
    /// Max number of requests in the queue
    #[serde(
        default = "ConfigRpc::default_request_channel_capacity",
        deserialize_with = "deserialize_num_str"
    )]
    pub request_channel_capacity: usize,
    /// In case of removed data upstream would be used to fetch block
    #[serde(default)]
    pub upstream: Option<ConfigRpcUpstream>,
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

    const fn default_request_channel_capacity() -> usize {
        4096
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub enum ConfigRpcCall {
    GetBlock,
    GetSlot,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ConfigRpcUpstream {
    pub endpoint: String,
    pub user_agent: String,
    #[serde(deserialize_with = "ConfigRpcUpstream::deserialize_version")]
    pub version: Version,
    #[serde(deserialize_with = "deserialize_num_str")]
    pub retries_max: usize,
    #[serde(with = "humantime_serde")]
    pub retries_backoff_init: Duration,
}

impl Default for ConfigRpcUpstream {
    fn default() -> Self {
        Self {
            endpoint: "http://127.0.0.1:8899".to_owned(),
            user_agent: format!("alpamayo/v{}", VERSION.package),
            version: Version::default(),
            retries_max: 3,
            retries_backoff_init: Duration::from_millis(100),
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
                return Err(de::Error::custom(format!(
                    "unknown HTTP version: {}",
                    value
                )));
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
