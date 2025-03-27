use {
    crate::{
        config::ConfigRpcWorkers,
        rpc::api_solana::{RpcRequestGetBlockWorkRequest, RpcRequestGetTransactionWorkRequest},
    },
    crossbeam::channel::{self, Receiver, RecvTimeoutError, Sender},
    futures::future::{BoxFuture, FutureExt, TryFutureExt, try_join_all},
    richat_shared::shutdown::Shutdown,
    std::{thread::Builder, time::Duration},
    tokio::{task::JoinError, time::sleep},
};

pub enum WorkRequest {
    Block(RpcRequestGetBlockWorkRequest),
    Transaction(RpcRequestGetTransactionWorkRequest),
}

#[allow(clippy::type_complexity)]
pub fn start(
    config: ConfigRpcWorkers,
    shutdown: Shutdown,
) -> anyhow::Result<(
    Sender<WorkRequest>,
    BoxFuture<'static, Result<(), JoinError>>,
)> {
    anyhow::ensure!(config.threads > 0, "number of threads can't be zero");

    let (tx, rx) = channel::bounded(config.channel_size);

    let mut jhs = Vec::with_capacity(config.threads);
    for index in 0..config.threads {
        let cpus = config.affinity.as_ref().map(|affinity| {
            if config.threads == affinity.len() {
                vec![affinity[index]]
            } else {
                affinity.clone()
            }
        });

        let th = Builder::new().name(format!("alpRpcWrk{index:02}")).spawn({
            let rx = rx.clone();
            let shutdown = shutdown.clone();

            move || {
                if let Some(cpus) = cpus {
                    affinity::set_thread_affinity(cpus).expect("failed to set affinity");
                }

                wrk_loop(rx, shutdown)
            }
        })?;

        let jh = tokio::spawn({
            let shutdown = shutdown.clone();

            async move {
                while !th.is_finished() {
                    let ms = if shutdown.is_set() { 10 } else { 500 };
                    sleep(Duration::from_millis(ms)).await;
                }
                th.join().expect("failed to join thread")
            }
        });
        jhs.push(jh);
    }

    Ok((tx, try_join_all(jhs).map_ok(|_| ()).boxed()))
}

fn wrk_loop(rx: Receiver<WorkRequest>, shutdown: Shutdown) {
    loop {
        let request = match rx.recv_timeout(Duration::from_millis(500)) {
            Ok(request) => request,
            Err(RecvTimeoutError::Timeout) => {
                if shutdown.is_set() {
                    return;
                } else {
                    continue;
                }
            }
            Err(RecvTimeoutError::Disconnected) => return,
        };

        match request {
            WorkRequest::Block(request) => request.process(),
            WorkRequest::Transaction(request) => request.process(),
        }
    }
}
