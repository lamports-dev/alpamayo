use {
    crate::{
        config::ConfigRpc,
        rpc::{api_rest, api_solana, workers},
        storage::{read::ReadRequest, slots::StoredSlots},
    },
    futures::future::{TryFutureExt, ready},
    http_body_util::{BodyExt, Empty as BodyEmpty},
    hyper::{Request, Response, StatusCode, body::Incoming as BodyIncoming, service::service_fn},
    hyper_util::{
        rt::tokio::{TokioExecutor, TokioIo},
        server::conn::auto::Builder as ServerBuilder,
    },
    richat_shared::shutdown::Shutdown,
    std::sync::Arc,
    tokio::{net::TcpListener, sync::mpsc, task::JoinError},
    tracing::{debug, error, info},
};

pub async fn spawn(
    config: ConfigRpc,
    stored_slots: StoredSlots,
    requests_tx: mpsc::Sender<ReadRequest>,
    shutdown: Shutdown,
) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
    let (workers_tx, workers_jhs) = workers::start(config.workers.clone(), shutdown.clone())?;

    let listener = TcpListener::bind(config.endpoint).await?;
    info!("start server at: {}", config.endpoint);

    let api_rest_state = Arc::new(api_rest::State::new(
        &config,
        stored_slots.clone(),
        requests_tx.clone(),
    )?);
    let api_solana_state = Arc::new(api_solana::State::new(
        config,
        stored_slots,
        requests_tx,
        workers_tx,
    )?);

    let jh = tokio::spawn(async move {
        let http = ServerBuilder::new(TokioExecutor::new());
        let graceful = hyper_util::server::graceful::GracefulShutdown::new();

        tokio::pin!(shutdown);
        loop {
            let stream = tokio::select! {
                incoming = listener.accept() => match incoming {
                    Ok((stream, addr)) => {
                        debug!("new connection from {addr}");
                        stream
                    }
                    Err(error) => {
                        error!("failed to accept new connection: {error}");
                        break;
                    }
                },
                () = &mut shutdown => break,
            };

            let service = service_fn({
                let api_rest_state = Arc::clone(&api_rest_state);
                let api_solana_state = Arc::clone(&api_solana_state);
                move |req: Request<BodyIncoming>| {
                    let api_rest_state = Arc::clone(&api_rest_state);
                    let api_solana_state = Arc::clone(&api_solana_state);
                    async move {
                        // JSON-RPC
                        if req.uri().path() == "/" {
                            return api_solana::on_request(req, api_solana_state).await;
                        }

                        // Rest (GET)
                        if let Some(handler) = api_rest_state.get_handler(req) {
                            return handler.await;
                        }

                        Response::builder()
                            .status(StatusCode::NOT_FOUND)
                            .body(BodyEmpty::new().boxed())
                    }
                }
            });

            let connection = http.serve_connection(TokioIo::new(stream), service);
            let fut = graceful.watch(connection.into_owned());

            tokio::spawn(async move {
                if let Err(error) = fut.await {
                    error!("Error serving HTTP connection: {error:?}");
                }
            });
        }

        drop(listener);
        graceful.shutdown().await;

        workers_jhs.await
    });

    Ok(jh.and_then(ready))
}
