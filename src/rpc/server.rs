use {
    crate::{config::ConfigRpc, rpc::api_solana, storage::read::ReadRequest},
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
    requests_tx: mpsc::Sender<ReadRequest>,
    shutdown: Shutdown,
) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
    let listener = TcpListener::bind(config.endpoint).await?;
    info!("start server at: {}", config.endpoint);

    let api_solana_state = Arc::new(api_solana::State::new(config, requests_tx)?);

    Ok(tokio::spawn(async move {
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
                let api_solana_state = Arc::clone(&api_solana_state);
                move |req: Request<BodyIncoming>| {
                    let api_solana_state = Arc::clone(&api_solana_state);
                    async move {
                        if req.uri().path() == "/" {
                            api_solana::on_request(req, api_solana_state).await
                        } else {
                            Response::builder()
                                .status(StatusCode::NOT_FOUND)
                                .body(BodyEmpty::new().boxed())
                        }
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
        graceful.shutdown().await
    }))
}
