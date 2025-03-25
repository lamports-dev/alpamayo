use {
    crate::{
        source::block::ConfirmedBlockWithBinary,
        storage::{
            blocks::{StorageBlockLocationResult, StoredBlocksRead},
            files::StorageFilesRead,
            slots::StoredConfirmedSlot,
            sync::ReadWriteSyncMessage,
        },
    },
    anyhow::Context,
    futures::{
        future::{FutureExt, LocalBoxFuture, pending},
        stream::{FuturesUnordered, StreamExt},
    },
    richat_shared::shutdown::Shutdown,
    solana_sdk::clock::Slot,
    std::{io, sync::Arc, thread, time::Instant},
    tokio::{
        sync::{Mutex, OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot},
        time::timeout_at,
    },
    tracing::error,
};

pub fn start(
    index: usize,
    affinity: Option<Vec<usize>>,
    sync_rx: broadcast::Receiver<ReadWriteSyncMessage>,
    read_requests_concurrency: Arc<Semaphore>,
    requests_rx: Arc<Mutex<mpsc::Receiver<ReadRequest>>>,
    stored_confirmed_slot: StoredConfirmedSlot,
    shutdown: Shutdown,
) -> anyhow::Result<thread::JoinHandle<anyhow::Result<()>>> {
    thread::Builder::new()
        .name(format!("alpStorageRd{index:02}"))
        .spawn(move || {
            tokio_uring::start(async move {
                if let Some(cpus) = affinity {
                    affinity::set_thread_affinity(&cpus).expect("failed to set affinity")
                }

                let mut read_requests = FuturesUnordered::new();
                let result = start2(
                    index,
                    sync_rx,
                    read_requests_concurrency,
                    requests_rx,
                    &mut read_requests,
                    stored_confirmed_slot,
                    shutdown,
                )
                .await;
                while read_requests.next().await.is_some() {}

                result
            })
        })
        .map_err(Into::into)
}

async fn start2(
    index: usize,
    mut sync_rx: broadcast::Receiver<ReadWriteSyncMessage>,
    read_requests_concurrency: Arc<Semaphore>,
    read_requests_rx: Arc<Mutex<mpsc::Receiver<ReadRequest>>>,
    read_requests: &mut FuturesUnordered<LocalBoxFuture<'_, ()>>,
    stored_confirmed_slot: StoredConfirmedSlot,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    tokio::pin!(shutdown);
    let (mut blocks, storage_files) = tokio::select! {
        message = sync_rx.recv() => {
            let ReadWriteSyncMessage::Init { blocks, storage_files_init } = message.context("failed to get sync init message")? else {
                anyhow::bail!("invalid sync message");
            };

            let storage_files = StorageFilesRead::open(storage_files_init).await.context("failed to open storage files")?;
            (blocks, storage_files)
        }
        () = &mut shutdown => return Ok(()),
    };

    let mut confirmed_in_process = None;

    let read_request_next =
        read_request_get_next(Arc::clone(&read_requests_concurrency), read_requests_rx);
    tokio::pin!(read_request_next);

    loop {
        let read_request_fut = if read_requests.is_empty() {
            pending().boxed_local()
        } else {
            read_requests.next().boxed_local()
        };

        tokio::select! {
            biased;
            // sync update
            message = sync_rx.recv() => match message.context("failed to get sync message")? {
                ReadWriteSyncMessage::Init { .. } => anyhow::bail!("unexpected second init"),
                ReadWriteSyncMessage::BlockNew { slot: _slot, block: _block } => continue,
                ReadWriteSyncMessage::BlockDead { slot: _slot } => continue,
                ReadWriteSyncMessage::BlockConfirmed { slot, block } => {
                    confirmed_in_process = Some((slot, block));
                    stored_confirmed_slot.set_confirmed(index, slot);
                },
                ReadWriteSyncMessage::ConfirmedBlockPop => blocks.pop_block(),
                ReadWriteSyncMessage::ConfirmedBlockPush { block } => {
                    let Some((slot, _block)) = confirmed_in_process.take() else {
                        anyhow::bail!("expected confirmed before push");
                    };
                    anyhow::ensure!(slot == block.slot(), "unexpect confirmed block: {slot} vs {}", block.slot());
                    blocks.push_block(block);
                },
            },
            // existed request
            message = read_request_fut => match message {
                Some(()) => continue,
                None => unreachable!(),
            },
            // get new request
            (read_requests_rx, lock, request) = &mut read_request_next => {
                read_request_next.set(read_request_get_next(
                    Arc::clone(&read_requests_concurrency),
                    read_requests_rx,
                ));
                let Some(request) = request else {
                    return Ok(());
                };

                if let Some(future) = request.process(lock, &storage_files, &blocks, &confirmed_in_process) {
                    read_requests.push(future);
                }
            },
            // shutdown
            () = &mut shutdown => return Ok(()),
        }
    }
}

async fn read_request_get_next(
    read_requests_concurrency: Arc<Semaphore>,
    read_requests_rx: Arc<Mutex<mpsc::Receiver<ReadRequest>>>,
) -> (
    Arc<Mutex<mpsc::Receiver<ReadRequest>>>,
    OwnedSemaphorePermit,
    Option<ReadRequest>,
) {
    let lock = read_requests_concurrency
        .acquire_owned()
        .await
        .expect("live semaphore");

    let mut rx = read_requests_rx.lock().await;
    let request = rx.recv().await;
    drop(rx);

    (read_requests_rx, lock, request)
}

#[derive(Debug)]
pub enum ReadResultGetBlock {
    Timeout,
    Removed,
    Dead,
    NotAvailable,
    Block(Vec<u8>),
    ReadError(io::Error),
}

#[derive(Debug)]
pub enum ReadRequest {
    GetBlock {
        deadline: Instant,
        slot: Slot,
        tx: oneshot::Sender<ReadResultGetBlock>,
    },
}

impl ReadRequest {
    pub fn process<'a>(
        self,
        lock: OwnedSemaphorePermit,
        files: &StorageFilesRead,
        blocks: &StoredBlocksRead,
        confirmed_in_process: &Option<(Slot, Option<ConfirmedBlockWithBinary>)>,
    ) -> Option<LocalBoxFuture<'a, ()>> {
        match self {
            Self::GetBlock { deadline, slot, tx } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultGetBlock::Timeout);
                    return None;
                }

                if let Some((confirmed_in_process_slot, confirmed_in_process_block)) =
                    confirmed_in_process
                {
                    if *confirmed_in_process_slot == slot {
                        let _ = tx.send(if let Some(block) = confirmed_in_process_block {
                            ReadResultGetBlock::Block(block.get_protobuf())
                        } else {
                            ReadResultGetBlock::Dead
                        });
                        return None;
                    }
                }

                let location = match blocks.get_block_location(slot) {
                    StorageBlockLocationResult::Removed => {
                        let _ = tx.send(ReadResultGetBlock::Removed);
                        return None;
                    }
                    StorageBlockLocationResult::Dead => {
                        let _ = tx.send(ReadResultGetBlock::Dead);
                        return None;
                    }
                    StorageBlockLocationResult::NotAvailable => {
                        let _ = tx.send(ReadResultGetBlock::NotAvailable);
                        return None;
                    }
                    StorageBlockLocationResult::SlotMismatch => {
                        error!(slot, "item/slot mismatch");
                        let _ = tx.send(ReadResultGetBlock::ReadError(io::Error::new(
                            io::ErrorKind::Other,
                            "item/slot mismatch",
                        )));
                        return None;
                    }
                    StorageBlockLocationResult::Found(location) => location,
                };

                let read_fut = files.read(location.storage_id, location.offset, location.size);
                let fut = async move {
                    let result = match timeout_at(deadline.into(), read_fut).await {
                        Ok(Ok(block)) => ReadResultGetBlock::Block(block),
                        Ok(Err(error)) => ReadResultGetBlock::ReadError(error),
                        Err(_error) => ReadResultGetBlock::Timeout,
                    };
                    let _ = tx.send(result);
                    drop(lock);
                }
                .boxed_local();
                Some(fut)
            }
        }
    }
}
