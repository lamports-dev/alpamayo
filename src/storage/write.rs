use {
    crate::{
        config::ConfigStorage,
        source::{
            block::ConfirmedBlockWithBinary,
            rpc::GetBlockError,
            stream::{StreamSourceMessage, StreamSourceSlotStatus},
        },
        storage::{
            blocks::StoredBlockHeaders,
            files::StorageFiles,
            memory::{MemoryConfirmedBlock, MemoryStorage},
            read::ReadRequest,
            slots::StoredSlots,
            source::{RpcRequest, RpcSourceConnected, RpcSourceConnectedError},
        },
    },
    anyhow::Context,
    futures::{
        future::{FutureExt, LocalBoxFuture, pending},
        stream::{FuturesUnordered, StreamExt},
    },
    richat_shared::shutdown::Shutdown,
    solana_sdk::clock::Slot,
    std::{
        collections::HashMap,
        sync::Arc,
        thread,
        time::{Duration, Instant},
    },
    tokio::{
        sync::{Notify, mpsc},
        task::{JoinHandle, spawn_local},
        time::sleep,
    },
    tracing::warn,
};

pub fn start(
    mut config: ConfigStorage,
    stored_slots: StoredSlots,
    rpc_tx: mpsc::Sender<RpcRequest>,
    stream_start: Arc<Notify>,
    stream_rx: mpsc::Receiver<StreamSourceMessage>,
    requests_rx: mpsc::Receiver<ReadRequest>,
    shutdown: Shutdown,
) -> anyhow::Result<thread::JoinHandle<anyhow::Result<()>>> {
    thread::Builder::new()
        .name("alpStorageWrite".to_owned())
        .spawn(move || {
            tokio_uring::start(async move {
                if let Some(cpus) = config.affinity {
                    affinity::set_thread_affinity(&cpus).expect("failed to set affinity")
                }

                let rpc_getblock_max_retries = config.blocks.rpc_getblock_max_retries;
                let rpc_getblock_backoff_init = config.blocks.rpc_getblock_backoff_init;
                let rpc_getblock_max_concurrency = config.blocks.rpc_getblock_max_concurrency;
                let rpc = RpcSourceConnected::new(rpc_tx);

                let blocks_files = std::mem::take(&mut config.blocks.files);
                let mut blocks_headers = StoredBlockHeaders::open(config.blocks).await?;
                let mut blocks_files = StorageFiles::open(blocks_files, &blocks_headers).await?;
                let memory_storage = MemoryStorage::default();

                stored_slots.first_available_store(blocks_headers.front_slot());

                let (mut read_requests, result) = start2(
                    stored_slots,
                    rpc_getblock_max_retries,
                    rpc_getblock_backoff_init,
                    rpc_getblock_max_concurrency,
                    rpc,
                    stream_start,
                    stream_rx,
                    requests_rx,
                    config.read_requests_concurrency,
                    &mut blocks_headers,
                    &mut blocks_files,
                    memory_storage,
                    shutdown,
                )
                .await;

                while read_requests.next().await.is_some() {}

                blocks_headers.close().await;
                blocks_files.close().await;

                result
            })
        })
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
async fn start2<'a>(
    stored_slots: StoredSlots,
    rpc_getblock_max_retries: usize,
    rpc_getblock_backoff_init: Duration,
    rpc_getblock_max_concurrency: usize,
    rpc: RpcSourceConnected,
    stream_start: Arc<Notify>,
    mut stream_rx: mpsc::Receiver<StreamSourceMessage>,
    mut read_requests_rx: mpsc::Receiver<ReadRequest>,
    read_requests_concurrency: usize,
    blocks_headers: &mut StoredBlockHeaders,
    blocks_files: &mut StorageFiles,
    mut memory_storage: MemoryStorage,
    shutdown: Shutdown,
) -> (FuturesUnordered<LocalBoxFuture<'a, ()>>, anyhow::Result<()>) {
    let mut read_requests = FuturesUnordered::new();

    // get block requests
    let mut rpc_requests = FuturesUnordered::new();
    #[allow(clippy::type_complexity)]
    let get_confirmed_block = |rpc_requests: &mut FuturesUnordered<
        JoinHandle<
            Result<(u64, Option<ConfirmedBlockWithBinary>), RpcSourceConnectedError<GetBlockError>>,
        >,
    >,
                               slot: Slot| {
        let mut max_retries = rpc_getblock_max_retries;
        let mut backoff_wait = rpc_getblock_backoff_init;
        let rpc = rpc.clone();
        rpc_requests.push(spawn_local(async move {
            loop {
                match rpc.get_block(slot).await {
                    Ok(block) => return Ok((slot, Some(block))),
                    Err(error) => {
                        if matches!(
                            error,
                            RpcSourceConnectedError::Error(GetBlockError::SlotSkipped(_))
                        ) {
                            return Ok((slot, None));
                        }
                        if max_retries == 0 {
                            return Err(error);
                        }
                        warn!(?error, slot, "failed to get confirmed block");
                        max_retries -= 1;
                        sleep(backoff_wait).await;
                        backoff_wait *= 2;
                    }
                }
            }
        }));
    };

    // queue of confirmed blocks
    let mut queued_slots = HashMap::<Slot, Option<ConfirmedBlockWithBinary>>::new();
    let mut queued_slots_backfilled = false;

    // fill the gap between stored and new
    let mut next_confirmed_slot = match rpc.get_slot_confirmed().await {
        Ok(slot) => slot,
        Err(error) => return (read_requests, Err(error.into())),
    };
    if let Some(slot) = blocks_headers.get_latest_slot() {
        let mut next_confirmed_slot_last_update = Instant::now();
        let mut next_rpc_request_slot = slot + 1;
        let mut next_database_slot = slot + 1;

        loop {
            // update confirmed slot every 3s
            if next_confirmed_slot_last_update.elapsed() > Duration::from_secs(3) {
                next_confirmed_slot = match rpc.get_slot_confirmed().await {
                    Ok(slot) => slot,
                    Err(error) => return (read_requests, Err(error.into())),
                };
                next_confirmed_slot_last_update = Instant::now();
            }

            // break if we are close enough
            if next_database_slot + 2 >= next_confirmed_slot {
                break;
            }

            // get blocks
            while next_rpc_request_slot <= next_confirmed_slot
                && rpc_requests.len() < rpc_getblock_max_concurrency
            {
                get_confirmed_block(&mut rpc_requests, next_rpc_request_slot);
                next_rpc_request_slot += 1;
            }

            // push block into the queue
            match rpc_requests.next().await {
                Some(Ok(Ok((slot, block)))) => {
                    queued_slots.insert(slot, block);
                }
                Some(Ok(Err(error))) => {
                    return (
                        read_requests,
                        Err(error).context("failed to get confirmed block"),
                    );
                }
                Some(Err(error)) => {
                    return (
                        read_requests,
                        Err(error).context("failed to join spawned task"),
                    );
                }
                None => unreachable!(),
            }

            while let Some(block) = queued_slots.remove(&next_database_slot) {
                if let Err(error) = blocks_files
                    .push_block(next_database_slot, block, blocks_headers, &stored_slots)
                    .await
                {
                    return (read_requests, Err(error));
                }
                next_database_slot += 1;
            }
        }
    }
    stream_start.notify_one();

    tokio::pin!(shutdown);
    loop {
        let rpc_requests_next = if rpc_requests.is_empty() {
            pending().boxed_local()
        } else {
            rpc_requests.next().boxed_local()
        };

        let read_requests_next = if read_requests.len() < read_requests_concurrency {
            read_requests_rx.recv().boxed_local()
        } else {
            pending().boxed_local()
        };

        let read_request_fut = if read_requests.is_empty() {
            pending().boxed_local()
        } else {
            read_requests.next().boxed_local()
        };

        tokio::select! {
            biased;
            // insert block requested from rpc
            message = rpc_requests_next => match message {
                Some(Ok(Ok((slot, block)))) => {
                    queued_slots.insert(slot, block);
                },
                Some(Ok(Err(error))) => {
                    return (read_requests, Err(error).context("failed to get confirmed block"));
                },
                Some(Err(error)) => {
                    return (read_requests, Err(error).context("failed to join spawned task"));
                },
                None => unreachable!(),
            },
            // handle new block from the stream
            message = stream_rx.recv() => match message {
                Some(message) => {
                    // add message
                    match message {
                        StreamSourceMessage::Block { slot, block } => {
                            memory_storage.add_processed(slot, block);
                            stored_slots.processed_store(slot);
                        }
                        StreamSourceMessage::SlotStatus { slot, status, .. } => {
                            match status {
                                StreamSourceSlotStatus::Dead => memory_storage.set_dead(slot),
                                StreamSourceSlotStatus::Confirmed => memory_storage.set_confirmed(slot),
                                StreamSourceSlotStatus::Finalized =>  stored_slots.finalized_store(slot),
                            }
                        }
                    }

                    // get confirmed and push to the queue
                    while let Some(block) = memory_storage.pop_confirmed() {
                        if block.get_slot() < next_confirmed_slot {
                            continue;
                        }

                        if !queued_slots_backfilled && block.get_slot() > next_confirmed_slot {
                            queued_slots_backfilled = true;

                            if block.get_slot() - next_confirmed_slot > rpc_getblock_max_concurrency as u64 {
                                return (read_requests, Err(anyhow::anyhow!(
                                    "backfill is too big: slot {} / next_confirmed_slot {} / rpc_getblock_max_concurrency {}",
                                    block.get_slot(),
                                    next_confirmed_slot,
                                    rpc_getblock_max_concurrency,
                                )))
                            }

                            for slot in next_confirmed_slot..block.get_slot() {
                                get_confirmed_block(&mut rpc_requests, slot);
                            }
                        }

                        match block {
                            MemoryConfirmedBlock::Missed { slot } => {
                                get_confirmed_block(&mut rpc_requests, slot);
                            },
                            MemoryConfirmedBlock::Dead { slot } => {
                                queued_slots.insert(slot, None);
                            }
                            MemoryConfirmedBlock::Block { slot, block } => {
                                queued_slots.insert(slot, Some(block));
                            }
                        }
                    }
                }
                None => return (read_requests, Ok(())),
            },
            // process read request
            message = read_request_fut => match message {
                Some(()) => continue,
                None => unreachable!(),
            },
            // process NEW read request
            message = read_requests_next => match message {
                Some(request) => {
                    if let Some(future) = request.process(blocks_files, blocks_headers) {
                        read_requests.push(future);
                    }
                    continue;
                },
                None => return (read_requests, Ok(())),
            },
            () = &mut shutdown => return (read_requests, Ok(())),
        }

        // save blocks
        while let Some(block) = queued_slots.remove(&next_confirmed_slot) {
            if let Err(error) = blocks_files
                .push_block(next_confirmed_slot, block, blocks_headers, &stored_slots)
                .await
            {
                return (read_requests, Err(error));
            }
            next_confirmed_slot += 1;
        }
    }
}
