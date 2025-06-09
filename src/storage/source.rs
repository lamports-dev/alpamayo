use {
    crate::{
        config::{ConfigSource, ConfigSourceStream},
        source::{
            block::BlockWithBinary,
            http::{GetBlockError, HttpSource},
            stream::{StreamSource, StreamSourceMessage},
        },
    },
    futures::stream::StreamExt,
    richat_shared::shutdown::Shutdown,
    solana_client::client_error::ClientError,
    solana_sdk::clock::Slot,
    std::sync::Arc,
    thiserror::Error,
    tokio::{
        sync::{Notify, mpsc, oneshot},
        time::sleep,
    },
    tracing::error,
};

#[derive(Debug)]
pub enum HttpRequest {
    Slots {
        tx: oneshot::Sender<Result<(Slot, Slot), ClientError>>,
    },
    FirstAvailableBlock {
        tx: oneshot::Sender<Result<Slot, ClientError>>,
    },
    Block {
        slot: Slot,
        httpget: bool,
        tx: oneshot::Sender<Result<BlockWithBinary, GetBlockError>>,
    },
}

#[derive(Debug, Error)]
pub enum HttpSourceConnectedError<E> {
    #[error("send channel is closed")]
    SendError,
    #[error("recv channel is closed")]
    RecvError,
    #[error(transparent)]
    Error(#[from] E),
}

pub type HttpSourceConnectedResult<T, E> = Result<T, HttpSourceConnectedError<E>>;

#[derive(Debug, Clone)]
pub struct HttpSourceConnected {
    http_tx: mpsc::Sender<HttpRequest>,
}

impl HttpSourceConnected {
    pub fn new() -> (Self, mpsc::Receiver<HttpRequest>) {
        let (http_tx, http_rx) = mpsc::channel(1);
        let this = Self { http_tx };
        (this, http_rx)
    }

    async fn send<T, E>(
        &self,
        request: HttpRequest,
        rx: oneshot::Receiver<Result<T, E>>,
    ) -> HttpSourceConnectedResult<T, E> {
        if self.http_tx.send(request).await.is_err() {
            Err(HttpSourceConnectedError::SendError)
        } else {
            match rx.await {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(error)) => Err(HttpSourceConnectedError::Error(error)),
                Err(_) => Err(HttpSourceConnectedError::RecvError),
            }
        }
    }

    pub async fn get_slots(&self) -> HttpSourceConnectedResult<(Slot, Slot), ClientError> {
        let (tx, rx) = oneshot::channel();
        self.send(HttpRequest::Slots { tx }, rx).await
    }

    pub async fn get_block(
        &self,
        slot: Slot,
        httpget: bool,
    ) -> HttpSourceConnectedResult<BlockWithBinary, GetBlockError> {
        let (tx, rx) = oneshot::channel();
        self.send(HttpRequest::Block { slot, httpget, tx }, rx)
            .await
    }

    pub async fn get_first_available_block(&self) -> HttpSourceConnectedResult<Slot, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.send(HttpRequest::FirstAvailableBlock { tx }, rx).await
    }
}

pub async fn start(
    config: ConfigSource,
    mut http_rx: mpsc::Receiver<HttpRequest>,
    stream_start: Arc<Notify>,
    stream_tx: mpsc::Sender<StreamSourceMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    let http = Arc::new(HttpSource::new(config.http).await?);
    let stream = start_stream(config.stream, stream_tx, stream_start);

    tokio::pin!(shutdown);
    tokio::pin!(stream);

    let mut finished = false;
    while !finished {
        finished = tokio::select! {
            () = &mut shutdown => true,
            item = http_rx.recv() => handle_http(item, Arc::clone(&http)),
            result = &mut stream => return result,
        };
    }
    shutdown.shutdown();

    Ok(())
}

async fn start_stream(
    config: ConfigSourceStream,
    stream_tx: mpsc::Sender<StreamSourceMessage>,
    stream_start: Arc<Notify>,
) -> anyhow::Result<()> {
    let mut backoff_duration = config.reconnect.map(|c| c.backoff_max);
    let backoff_max = config.reconnect.map(|c| c.backoff_max).unwrap_or_default();

    stream_start.notified().await;
    loop {
        let mut stream = loop {
            match StreamSource::new(config.clone()).await {
                Ok(stream) => break stream,
                Err(error) => {
                    if let Some(sleep_duration) = backoff_duration {
                        error!(?error, "failed to connect to gRPC stream");
                        sleep(sleep_duration).await;
                        backoff_duration = Some(backoff_max.min(sleep_duration * 2));
                    } else {
                        return Err(error.into());
                    }
                }
            }
        };
        if stream_tx.send(StreamSourceMessage::Start).await.is_err() {
            error!("failed to send a message to the stream");
            return Ok(());
        }

        loop {
            match stream.next().await {
                Some(Ok(message)) => {
                    if stream_tx.send(message).await.is_err() {
                        error!("failed to send a message to the stream");
                        return Ok(());
                    }
                }
                Some(Err(error)) => {
                    error!(?error, "gRPC stream error");
                    break;
                }
                None => {
                    error!("gRPC stream is finished");
                    break;
                }
            }
        }

        if let Some(config) = config.reconnect {
            backoff_duration = Some(config.backoff_init);
        } else {
            return Ok(());
        }
    }
}

fn handle_http(item: Option<HttpRequest>, http: Arc<HttpSource>) -> bool {
    match item {
        Some(request) => {
            tokio::spawn(async move {
                match request {
                    HttpRequest::Slots { tx } => {
                        let result =
                            tokio::try_join!(http.get_finalized_slot(), http.get_confirmed_slot());
                        let _ = tx.send(result);
                    }
                    HttpRequest::FirstAvailableBlock { tx } => {
                        let result = http.get_first_available_block().await;
                        let _ = tx.send(result);
                    }
                    HttpRequest::Block { slot, httpget, tx } => {
                        let result = http.get_block(slot, httpget).await;
                        let _ = tx.send(result);
                    }
                }
            });
            false
        }
        None => {
            error!("RPC requests stream is finished");
            true
        }
    }
}
