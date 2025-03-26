use {
    crate::{
        config::ConfigSource,
        source::{
            block::BlockWithBinary,
            rpc::{GetBlockError, RpcSource},
            stream::{RecvError, StreamSource, StreamSourceMessage},
        },
    },
    futures::stream::StreamExt,
    richat_shared::shutdown::Shutdown,
    solana_client::client_error::ClientError,
    solana_sdk::clock::Slot,
    std::sync::Arc,
    thiserror::Error,
    tokio::sync::{Notify, mpsc, oneshot},
    tracing::error,
};

#[derive(Debug)]
pub enum RpcRequest {
    ConfirmedSlot {
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
    pub fn new(rpc_tx: mpsc::Sender<RpcRequest>) -> Self {
        Self { rpc_tx }
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

    pub async fn get_slot_confirmed(&self) -> RpcSourceConnectedResult<Slot, ClientError> {
        let (tx, rx) = oneshot::channel();
        self.send(RpcRequest::ConfirmedSlot { tx }, rx).await
    }

    pub async fn get_block(
        &self,
        slot: Slot,
    ) -> RpcSourceConnectedResult<BlockWithBinary, GetBlockError> {
        let (tx, rx) = oneshot::channel();
        self.send(RpcRequest::Block { slot, tx }, rx).await
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
    let mut stream: Option<StreamSource> = None;

    tokio::pin!(shutdown);
    let mut finished = false;
    while !finished {
        finished = if let Some(stream) = stream.as_mut() {
            tokio::select! {
                () = &mut shutdown => true,
                item = rpc_rx.recv() => handle_rpc(item, &rpc),
                item = stream.next() => handle_stream(item, &stream_tx).await
            }
        } else {
            tokio::select! {
                () = &mut shutdown => true,
                item = rpc_rx.recv() => handle_rpc(item, &rpc),
                () = stream_start.notified() => {
                    stream = Some(StreamSource::new(config.stream.clone()).await?);
                    false
                },
            }
        };
    }
    shutdown.shutdown();

    Ok(())
}

fn handle_rpc(item: Option<RpcRequest>, rpc: &Arc<RpcSource>) -> bool {
    match item {
        Some(request) => {
            let rpc = Arc::clone(rpc);
            tokio::spawn(async move {
                match request {
                    RpcRequest::ConfirmedSlot { tx } => {
                        let result = rpc.get_confirmed_slot().await;
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

async fn handle_stream(
    item: Option<Result<StreamSourceMessage, RecvError>>,
    stream_tx: &mpsc::Sender<StreamSourceMessage>,
) -> bool {
    match item {
        Some(Ok(message)) => {
            if stream_tx.send(message).await.is_err() {
                error!("failed to send a message to the stream");
                true
            } else {
                false
            }
        }
        Some(Err(error)) => {
            error!(?error, "gRPC stream error");
            true
        }
        None => {
            error!("gRPC stream is finished");
            true
        }
    }
}
