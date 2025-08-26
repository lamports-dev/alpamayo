use {
    crate::{config::ConfigMetrics, storage::slots::StoredSlots, version::VERSION as VERSION_INFO},
    anyhow::Context,
    metrics::{counter, describe_counter, describe_gauge, describe_histogram},
    metrics_exporter_prometheus::{Matcher, PrometheusBuilder, PrometheusHandle},
    richat_shared::jsonrpc::metrics::{
        RPC_REQUESTS_DURATION_SECONDS, describe as describe_jsonrpc_metrics,
    },
    std::{future::Future, time::Duration},
    tokio::{task::JoinError, time::sleep},
};

pub const STORAGE_STORED_SLOTS: &str = "storage_stored_slots"; // type
pub const STORAGE_FILES_SPACE: &str = "storage_files_space_bytes"; // id, type

pub const READ_DISK_SECONDS_TOTAL: &str = "read_disk_seconds_total"; // x_subscription_id, type

pub const WRITE_BLOCK_SYNC_SECONDS: &str = "write_block_sync_seconds";

pub const RPC_WORKERS_CPU_SECONDS_TOTAL: &str = "rpc_workers_cpu_seconds_total"; // x_subscription_id, method

pub const RPC_UPSTREAM_REQUESTS_TOTAL: &str = "rpc_upstream_requests_total"; // x_subscription_id, method

pub fn setup() -> anyhow::Result<PrometheusHandle> {
    let handle = PrometheusBuilder::new()
        .set_buckets_for_metric(
            Matcher::Full(WRITE_BLOCK_SYNC_SECONDS.to_owned()),
            &[0.005, 0.01, 0.02, 0.03, 0.04, 0.05, 0.075, 0.1, 0.2, 0.4],
        )?
        .set_buckets_for_metric(
            Matcher::Full(RPC_REQUESTS_DURATION_SECONDS.to_owned()),
            &[
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
            ],
        )?
        .install_recorder()
        .context("failed to install prometheus exporter")?;

    describe_jsonrpc_metrics();

    describe_counter!("version", "Alpamayo version info");
    counter!(
        "version",
        "buildts" => VERSION_INFO.buildts,
        "git" => VERSION_INFO.git,
        "package" => VERSION_INFO.package,
        "proto_dragonsmouth" => VERSION_INFO.proto,
        "proto_richat" => VERSION_INFO.proto_richat,
        "rustc" => VERSION_INFO.rustc,
        "solana" => VERSION_INFO.solana,
        "version" => VERSION_INFO.version,
    )
    .absolute(1);

    describe_gauge!(STORAGE_STORED_SLOTS, "Stored slots in db");
    describe_gauge!(STORAGE_FILES_SPACE, "Storage space in files for blocks");

    describe_gauge!(
        READ_DISK_SECONDS_TOTAL,
        "Read disk time by x-subscription-id and type"
    );

    describe_histogram!(WRITE_BLOCK_SYNC_SECONDS, "Write block sync time");

    describe_gauge!(
        RPC_WORKERS_CPU_SECONDS_TOTAL,
        "CPU consumption by RPC workers by x-subscription-id and method"
    );

    describe_counter!(
        RPC_UPSTREAM_REQUESTS_TOTAL,
        "Number of RPC requests to upstream by x-subscription-id and method"
    );

    Ok(handle)
}

pub async fn spawn_server(
    config: ConfigMetrics,
    handle: PrometheusHandle,
    stored_slots: StoredSlots,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
    let recorder_handle = handle.clone();
    tokio::spawn(async move {
        loop {
            sleep(Duration::from_secs(1)).await;
            recorder_handle.run_upkeep();
        }
    });

    richat_metrics::spawn_server(
        richat_metrics::ConfigMetrics {
            endpoint: config.endpoint,
        },
        move || handle.render().into_bytes(), // metrics
        || true,                              // health
        move || stored_slots.is_ready(),      // ready
        shutdown,
    )
    .await
    .map_err(Into::into)
}
