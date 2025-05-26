use {
    crate::{
        config::{ConfigSource, ConfigSourceStream},
        source::{
            block::BlockWithBinary,
            rpc::{GetBlockError, RpcSource},
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
pub enum RpcRequest {
    Slots {
        tx: oneshot::Sender<Result<(Slot, Slot), ClientError>>,
    },
    FirstAvailableBlock {
        tx: oneshot::Sender<Result<Slot, ClientError>>,
    },
    Block {
        slot: Slot,
        tx: oneshot::Sender<Result<BlockWithBinary, GetBlockError>>,
    },
}

#[derive(Debug, Error)]
pub enum RpcSourceConnectedError<E> {
    #[error("send channel is closed")]
    SendError,
    #[error("recv channel is closed")]
    RecvError,
    #[error(transparent)]
    Error(#[from] E),
}

pub type RpcSourceConnectedResult<T, E> = Result<T, RpcSourceConnectedError<E>>;

#[derive(Debug, Clone)]
pub struct RpcSourceConnected {
    rpc_tx: mpsc::Sender<RpcRequest>,
}

impl RpcSourceConnected {
    pub fn new() -> (Self, mpsc::Receiver<RpcRequest>) {
        let (rpc_tx, rpc_rx) = mpsc::channel(1);
        let this = Self { rpc_tx };
        (this, rpc_rx)
    }

    async fn send<T, E>(
        &self,
        request: RpcRequest,
        rx: oneshot::Receiver<Result<T, E>>,
    ) -> RpcSourceConnectedResult<T, E> {
        if self.rpc_tx.send(request).await.is_err() {
            Err(RpcSourceConnectedError::SendError)
        } else {
            match rx.await {
                Ok(Ok(result)) => Ok(result),
                Ok(Err(error)) => Err(RpcSourceConnectedError::Error(error)),
                Err(_) => Err(RpcSourceConnectedError::RecvError),
            }
        }
    }

    pub async fn get_slots(&self) -> RpcSourceConnectedResult<(Slot, Slot), ClientError> {
        let (tx, rx) = oneshot::channel();
        self.send(RpcRequest::Slots { tx }, rx).await
    }

    pub async fn get_block(
        &self,
        slot: Slot,
    ) -> RpcSourceConnectedResult<BlockWithBinary, GetBlockError> {
        let (tx, rx) = oneshot::channel();
        self.send(RpcRequest::Block { slot, tx }, rx).await
    }

    pub async fn get_first_available_block(&self) -> RpcSourceConnectedResult<Slot, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.send(RpcRequest::FirstAvailableBlock { tx }, rx).await
    }
}

pub async fn start(
    config: ConfigSource,
    mut rpc_rx: mpsc::Receiver<RpcRequest>,
    stream_start: Arc<Notify>,
    stream_tx: mpsc::Sender<StreamSourceMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    let rpc = Arc::new(RpcSource::new(config.rpc).await?);
    let stream = start_stream(config.stream, stream_tx, stream_start);

    tokio::pin!(shutdown);
    tokio::pin!(stream);

    let mut finished = false;
    while !finished {
        finished = tokio::select! {
            () = &mut shutdown => true,
            item = rpc_rx.recv() => handle_rpc(item, Arc::clone(&rpc)),
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

fn handle_rpc(item: Option<RpcRequest>, rpc: Arc<RpcSource>) -> bool {
    match item {
        Some(request) => {
            tokio::spawn(async move {
                match request {
                    RpcRequest::Slots { tx } => {
                        let result =
                            tokio::try_join!(rpc.get_finalized_slot(), rpc.get_confirmed_slot());
                        let _ = tx.send(result);
                    }
                    RpcRequest::FirstAvailableBlock { tx } => {
                        let result = rpc.get_first_available_block().await;
                        let _ = tx.send(result);
                    }
                    RpcRequest::Block { slot, tx } => {
                        let result = rpc.get_block(slot).await;
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
