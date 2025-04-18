use {
    crate::{
        config::ConfigStorage,
        metrics::{WRITE_BLOCK_SYNC_SECONDS, duration_to_seconds},
        source::{
            block::BlockWithBinary,
            rpc::GetBlockError,
            stream::{StreamSourceMessage, StreamSourceSlotStatus},
        },
        storage::{
            blocks::StoredBlocksWrite,
            files::{StorageFilesRead, StorageFilesWrite},
            memory::{MemoryConfirmedBlock, StorageMemory},
            rocksdb::{RocksdbRead, RocksdbWrite},
            slots::StoredSlots,
            source::{RpcRequest, RpcSourceConnected, RpcSourceConnectedError},
            sync::ReadWriteSyncMessage,
        },
        util::HashMap,
    },
    anyhow::Context,
    futures::{
        future::{FutureExt, pending, try_join_all},
        stream::{FuturesUnordered, StreamExt},
    },
    metrics::histogram,
    prost::Message,
    quanta::Instant,
    rayon::{
        ThreadPoolBuilder,
        iter::{IntoParallelIterator, ParallelIterator},
    },
    richat_shared::shutdown::Shutdown,
    solana_sdk::clock::Slot,
    solana_storage_proto::convert::generated,
    solana_transaction_status::ConfirmedBlock,
    std::{sync::Arc, thread, time::Duration},
    tokio::{
        sync::{Notify, broadcast, mpsc},
        task::{JoinHandle, spawn_local},
        time::sleep,
    },
    tracing::{info, warn},
};

#[allow(clippy::too_many_arguments)]
pub fn start(
    config: ConfigStorage,
    stored_slots: StoredSlots,
    db_write: RocksdbWrite,
    db_read: RocksdbRead,
    rpc_tx: mpsc::Sender<RpcRequest>,
    stream_start: Arc<Notify>,
    stream_rx: mpsc::Receiver<StreamSourceMessage>,
    sync_tx: broadcast::Sender<ReadWriteSyncMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<thread::JoinHandle<anyhow::Result<()>>> {
    thread::Builder::new()
        .name("alpStorageWrt".to_owned())
        .spawn(move || {
            let recent_blocks_thread_pool = ThreadPoolBuilder::new()
                .build()
                .context("failed to build thread pool to decode recent blocks")?;

            tokio_uring::start(async move {
                if let Some(cpus) = config.write.affinity {
                    affinity_linux::set_thread_affinity(cpus.into_iter())
                        .expect("failed to set affinity")
                }

                let rpc_getblock_max_retries = config.blocks.rpc_getblock_max_retries;
                let rpc_getblock_backoff_init = config.blocks.rpc_getblock_backoff_init;
                let rpc_getblock_max_concurrency = config.blocks.rpc_getblock_max_concurrency;
                let rpc = RpcSourceConnected::new(rpc_tx);

                let files = config.blocks.files.clone();
                let blocks = StoredBlocksWrite::new(
                    db_read.read_slot_indexes()?.await?,
                    config.blocks.max,
                    stored_slots.clone(),
                    sync_tx.clone(),
                )?;
                let (mut storage_files, storage_files_read_sync_init) =
                    StorageFilesWrite::open(files, &blocks).await?;
                let storage_memory = StorageMemory::default();

                // load recent blocks
                let ts = Instant::now();
                let recent_blocks =
                    match try_join_all(blocks.get_recent_blocks().into_iter().map(|stored_block| {
                        let storage_files_init = storage_files_read_sync_init.clone();
                        async move {
                            let storage_files = StorageFilesRead::open(storage_files_init)
                                .await
                                .context("failed to open storage files")?;

                            storage_files
                                .read(
                                    stored_block.storage_id,
                                    stored_block.offset,
                                    stored_block.size,
                                )
                                .await
                                .context("failed to read block buffer")
                                .map(|bytes| (stored_block.slot, bytes))
                        }
                    }))
                    .await
                    .context("failed to load recent blocks")
                    .and_then(|items| {
                        recent_blocks_thread_pool.install(|| {
                            items
                                .into_par_iter()
                                .map(|(slot, bytes)| {
                                    match generated::ConfirmedBlock::decode(bytes.as_ref())
                                    .context("failed to decode protobuf")
                                {
                                    Ok(block) => match ConfirmedBlock::try_from(block)
                                        .context("failed to convert to confirmed block")
                                    {
                                        Ok(block) => Ok((
                                            slot,
                                            Arc::new(
                                                BlockWithBinary::new_from_confirmed_block_and_slot(
                                                    block, slot,
                                                ),
                                            ),
                                        )),
                                        Err(error) => Err(error),
                                    },
                                    Err(error) => Err(error),
                                }
                                })
                                .collect::<Result<Vec<_>, anyhow::Error>>()
                        })
                    }) {
                        Ok(recent_blocks) => recent_blocks,
                        Err(error) => {
                            storage_files.close().await;
                            return Err(error);
                        }
                    };
                info!(len = recent_blocks.len(), elapsed = ?ts.elapsed(), "load recent blocks");
                drop(recent_blocks_thread_pool);

                // notify readers
                sync_tx
                    .send(ReadWriteSyncMessage::Init {
                        blocks: blocks.to_read(),
                        db_read: db_read.clone(),
                        storage_files_init: storage_files_read_sync_init,
                        recent_blocks,
                    })
                    .context("failed to send read/write init message")?;

                let result = start2(
                    stored_slots,
                    rpc_getblock_max_retries,
                    rpc_getblock_backoff_init,
                    rpc_getblock_max_concurrency,
                    rpc,
                    stream_start,
                    stream_rx,
                    blocks,
                    db_write,
                    &mut storage_files,
                    storage_memory,
                    sync_tx,
                    shutdown,
                )
                .await;

                storage_files.close().await;

                result
            })
        })
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
async fn start2(
    stored_slots: StoredSlots,
    rpc_getblock_max_retries: usize,
    rpc_getblock_backoff_init: Duration,
    rpc_getblock_max_concurrency: usize,
    rpc: RpcSourceConnected,
    stream_start: Arc<Notify>,
    mut stream_rx: mpsc::Receiver<StreamSourceMessage>,
    mut blocks: StoredBlocksWrite,
    db_write: RocksdbWrite,
    storage_files: &mut StorageFilesWrite,
    mut storage_memory: StorageMemory,
    sync_tx: broadcast::Sender<ReadWriteSyncMessage>,
    shutdown: Shutdown,
) -> anyhow::Result<()> {
    // get block requests
    let mut rpc_requests = FuturesUnordered::new();
    #[allow(clippy::type_complexity)]
    let get_confirmed_block = |rpc_requests: &mut FuturesUnordered<
        JoinHandle<Result<(u64, Option<BlockWithBinary>), RpcSourceConnectedError<GetBlockError>>>,
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

    let metric_storage_block_sync = histogram!(WRITE_BLOCK_SYNC_SECONDS);

    // queue of confirmed blocks
    let mut queued_slots = HashMap::<Slot, Option<Arc<BlockWithBinary>>>::default();
    let mut queued_slots_backfilled = false;

    // fill the gap between stored and new
    let mut next_confirmed_slot = match rpc.get_slot_confirmed().await {
        Ok(slot) => slot,
        Err(error) => return Err(error.into()),
    };
    if let Some(slot) = blocks.get_latest_slot() {
        let mut next_confirmed_slot_last_update = Instant::now();
        let mut next_rpc_request_slot = slot + 1;
        let mut next_database_slot = slot + 1;
        info!(
            slot_db = next_database_slot,
            slot_node = next_confirmed_slot,
            diff = next_confirmed_slot - next_database_slot,
            "initiate node catch-up process"
        );
        anyhow::ensure!(
            next_database_slot <= next_confirmed_slot,
            "node is outdated"
        );

        loop {
            // update confirmed slot every 3s
            if next_confirmed_slot_last_update.elapsed() > Duration::from_secs(3) {
                next_confirmed_slot = match rpc.get_slot_confirmed().await {
                    Ok(slot) => slot,
                    Err(error) => return Err(error.into()),
                };
                next_confirmed_slot_last_update = Instant::now();
                info!(
                    slot_db = next_database_slot,
                    slot_node = next_confirmed_slot,
                    diff = next_confirmed_slot - next_database_slot,
                    "trying to catch-up the node"
                );
            }

            // break if we are close enough
            if next_database_slot + 2 >= next_confirmed_slot {
                next_confirmed_slot = next_database_slot;
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
                    queued_slots.insert(slot, block.map(Arc::new));
                }
                Some(Ok(Err(error))) => {
                    return Err(error).context("failed to get confirmed block");
                }
                Some(Err(error)) => {
                    return Err(error).context("failed to join spawned task");
                }
                None => unreachable!(),
            }

            while let Some(block) = queued_slots.remove(&next_database_slot) {
                let _ = sync_tx.send(ReadWriteSyncMessage::BlockConfirmed {
                    slot: next_database_slot,
                    block: block.clone(),
                });

                let ts = Instant::now();
                db_write
                    .push_block(next_database_slot, block, storage_files, &mut blocks)
                    .await?;
                metric_storage_block_sync.record(duration_to_seconds(ts.elapsed()));

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

        tokio::select! {
            biased;
            // insert block requested from rpc
            message = rpc_requests_next => match message {
                Some(Ok(Ok((slot, block)))) => {
                    queued_slots.insert(slot, block.map(Arc::new));
                },
                Some(Ok(Err(error))) => {
                    return Err(error).context("failed to get confirmed block");
                },
                Some(Err(error)) => {
                    return Err(error).context("failed to join spawned task");
                },
                None => unreachable!(),
            },
            // handle new block from the stream
            message = stream_rx.recv() => match message {
                Some(message) => {
                    // add message
                    match message {
                        StreamSourceMessage::Block { slot, block } => {
                            let block = Arc::new(block);
                            storage_memory.add_processed(slot, Arc::clone(&block));
                            let _ = sync_tx.send(ReadWriteSyncMessage::BlockNew { slot, block });
                            stored_slots.processed_store(slot);
                        }
                        StreamSourceMessage::SlotStatus { slot, status, .. } => {
                            match status {
                                StreamSourceSlotStatus::Dead => {
                                    storage_memory.set_dead(slot);
                                    let _ = sync_tx.send(ReadWriteSyncMessage::BlockDead { slot });
                                },
                                StreamSourceSlotStatus::Confirmed => storage_memory.set_confirmed(slot),
                                StreamSourceSlotStatus::Finalized => {
                                    let _ = sync_tx.send(ReadWriteSyncMessage::SlotFinalized { slot });
                                },
                            }
                        }
                        StreamSourceMessage::SlotStatusInit { slot, status } => {
                            if slot >= stored_slots.first_available_load() {
                                match status {
                                    StreamSourceSlotStatus::Confirmed => stored_slots.confirmed_store(slot),
                                    StreamSourceSlotStatus::Finalized => {
                                        let _ = sync_tx.send(ReadWriteSyncMessage::SlotFinalized { slot });
                                    }
                                    _ => unreachable!(),
                                }
                            }
                        }
                    }

                    // get confirmed and push to the queue
                    while let Some(block) = storage_memory.pop_confirmed() {
                        if block.get_slot() < next_confirmed_slot {
                            continue;
                        }

                        if !queued_slots_backfilled && block.get_slot() > next_confirmed_slot {
                            queued_slots_backfilled = true;

                            if block.get_slot() - next_confirmed_slot > rpc_getblock_max_concurrency as u64 {
                                return Err(anyhow::anyhow!(
                                    "backfill is too big: slot {} / next_confirmed_slot {} / rpc_getblock_max_concurrency {}",
                                    block.get_slot(),
                                    next_confirmed_slot,
                                    rpc_getblock_max_concurrency,
                                ))
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
                None => return Ok(()),
            },
            () = &mut shutdown => return Ok(()),
        }

        // save blocks
        while let Some(block) = queued_slots.remove(&next_confirmed_slot) {
            let _ = sync_tx.send(ReadWriteSyncMessage::BlockConfirmed {
                slot: next_confirmed_slot,
                block: block.clone(),
            });

            let ts = Instant::now();
            db_write
                .push_block(next_confirmed_slot, block, storage_files, &mut blocks)
                .await?;
            metric_storage_block_sync.record(duration_to_seconds(ts.elapsed()));

            next_confirmed_slot += 1;
        }
    }
}
