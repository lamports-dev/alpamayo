use {
    alpamayo::{config::Config, metrics, rpc, storage},
    anyhow::Context,
    clap::Parser,
    futures::future::{FutureExt, TryFutureExt, ready, try_join_all},
    quanta::Instant,
    richat_shared::shutdown::Shutdown,
    signal_hook::{consts::SIGINT, iterator::Signals},
    std::{
        sync::Arc,
        thread::{self, sleep},
        time::Duration,
    },
    tokio::sync::{Mutex, Notify, Semaphore, broadcast, mpsc},
    tracing::{info, warn},
};

#[derive(Debug, Parser)]
#[clap(
    author,
    version,
    about = "Alpamayo: part of Solana RPC stack for sealed data"
)]
struct Args {
    /// Path to config
    #[clap(short, long, default_value_t = String::from("config.yml"))]
    pub config: String,

    /// Only check config and exit
    #[clap(long, default_value_t = false)]
    pub check: bool,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    let config = Config::load_from_file(&args.config)
        .with_context(|| format!("failed to load config from {}", args.config))?;

    // Setup logs
    alpamayo::log::setup(config.logs.json)?;
    let metrics_handle = metrics::setup()?;

    // Exit if we only check the config
    if args.check {
        info!("Config is OK!");
        return Ok(());
    }

    // Shutdown channel/flag
    let mut threads = Vec::<(String, _)>::with_capacity(8);
    let shutdown = Shutdown::new();

    // Source / storage write channels
    let stream_start = Arc::new(Notify::new());
    let (stream_tx, stream_rx) = mpsc::channel(2048);
    let (rpc_tx, rpc_rx) = mpsc::channel(2048);

    // Storage write / storage read channels
    let (sync_tx, _) = broadcast::channel(1024);

    // Storage read / rpc channels
    let stored_slots = storage::slots::StoredSlots::default();
    let (read_requests_tx, read_requests_rx) = mpsc::channel(config.rpc.request_channel_capacity);

    // Open Rocksdb for slots and indexes
    let ts = Instant::now();
    let (db_write, db_read, db_threads) =
        storage::rocksdb::Rocksdb::open(config.storage.rocksdb.clone(), sync_tx.clone())?;
    info!(elapsed = ?ts.elapsed(), "rocksdb opened");
    threads.extend(db_threads);

    // Create source runtime
    let jh = thread::Builder::new().name("alpSource".to_owned()).spawn({
        let stream_start = Arc::clone(&stream_start);
        let stored_slots = stored_slots.clone();
        let shutdown = shutdown.clone();
        move || {
            let runtime = config.source.tokio.clone().build_runtime("alpSourceRt")?;
            runtime.block_on(async move {
                let source_fut = tokio::spawn(storage::source::start(
                    config.source,
                    rpc_rx,
                    stream_start,
                    stream_tx,
                    shutdown.clone(),
                ))
                .map_err(Into::into)
                .and_then(ready)
                .boxed();

                let server_fut =
                    metrics::spawn_server(config.metrics, metrics_handle, stored_slots, shutdown)
                        .await?
                        .map_err(anyhow::Error::from)
                        .boxed();

                try_join_all(vec![source_fut, server_fut]).await.map(|_| ())
            })
        }
    })?;
    threads.push(("alpSource".to_owned(), Some(jh)));

    // Storage read runtimes
    let read_requests_rx = Arc::new(Mutex::new(read_requests_rx));
    let read_requests_concurrency = Arc::new(Semaphore::const_new(
        config.storage.read.requests_concurrency,
    ));
    let stored_confirmed_slot =
        storage::slots::StoredSlotsRead::new(stored_slots.clone(), config.storage.read.threads);
    for index in 0..config.storage.read.threads {
        let affinity = config.storage.read.affinity.as_ref().map(|affinity| {
            if affinity.len() == config.storage.read.threads {
                vec![affinity[index]]
            } else {
                affinity.clone()
            }
        });

        let jh = storage::read::start(
            index,
            affinity,
            sync_tx.subscribe(),
            Arc::clone(&read_requests_concurrency),
            Arc::clone(&read_requests_rx),
            stored_confirmed_slot.clone(),
        )?;
        threads.push((format!("alpStorageRd{index:02}"), Some(jh)));
    }
    drop(read_requests_rx);
    drop(read_requests_concurrency);

    // Storage write runtime
    let jh = storage::write::start(
        config.storage.clone(),
        stored_slots.clone(),
        db_write,
        db_read,
        rpc_tx,
        stream_start,
        stream_rx,
        sync_tx,
        shutdown.clone(),
    )?;
    threads.push(("alpStorageWrt".to_owned(), Some(jh)));

    // Rpc runtime
    let jh = thread::Builder::new().name("alpRpc".to_owned()).spawn({
        let shutdown = shutdown.clone();
        move || {
            let runtime = config.rpc.tokio.clone().build_runtime("alpRpcRt")?;
            runtime.block_on(async move {
                rpc::server::spawn(config.rpc, stored_slots, read_requests_tx, shutdown.clone())
                    .await?
                    .await?;
                Ok::<(), anyhow::Error>(())
            })
        }
    })?;
    threads.push(("alpRpc".to_owned(), Some(jh)));

    // Shutdown loop
    let mut signals = Signals::new([SIGINT])?;
    'outer: while threads.iter().any(|th| th.1.is_some()) {
        for signal in signals.pending() {
            match signal {
                SIGINT => {
                    if shutdown.is_set() {
                        warn!("SIGINT received again, shutdown now");
                        break 'outer;
                    }
                    info!("SIGINT received...");
                    shutdown.shutdown();
                }
                _ => unreachable!(),
            }
        }

        for (name, tjh) in threads.iter_mut() {
            if let Some(jh) = tjh.take() {
                if jh.is_finished() {
                    jh.join()
                        .unwrap_or_else(|_| panic!("{name} thread join failed"))?;
                    info!("thread {name} finished");
                } else {
                    *tjh = Some(jh);
                }
            }
        }

        sleep(Duration::from_millis(25));
    }

    Ok(())
}
