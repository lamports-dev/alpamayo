use {
    crate::{
        rpc::api_solana::RpcRequestBlocksUntil,
        source::{block::BlockWithBinary, fees::TransactionsFees},
        storage::{
            blocks::{StorageBlockLocationResult, StoredBlocksRead},
            files::StorageFilesRead,
            rocksdb::{RocksdbRead, TransactionIndexValue},
            slots::StoredConfirmedSlot,
            sync::ReadWriteSyncMessage,
        },
        util::HashMap,
    },
    anyhow::Context,
    futures::{
        future::{FutureExt, LocalBoxFuture, pending, ready},
        stream::{FuturesUnordered, StreamExt},
    },
    solana_rpc_client_api::response::{
        RpcConfirmedTransactionStatusWithSignature, RpcPrioritizationFee,
    },
    solana_sdk::{
        clock::{MAX_PROCESSING_AGE, MAX_RECENT_BLOCKHASHES, Slot, UnixTimestamp},
        commitment_config::{CommitmentConfig, CommitmentLevel},
        pubkey::Pubkey,
        signature::Signature,
        transaction::TransactionError,
    },
    solana_transaction_status::{TransactionConfirmationStatus, TransactionStatus},
    std::{
        collections::{BTreeMap, btree_map::Entry as BTreeMapEntry},
        io,
        sync::Arc,
        thread,
        time::Instant,
    },
    tokio::{
        sync::{Mutex, OwnedSemaphorePermit, Semaphore, broadcast, mpsc, oneshot},
        time::timeout_at,
    },
    tracing::error,
};

const RPF_MAX_NUM_RECENT_BLOCKS: usize = 150;

pub fn start(
    index: usize,
    affinity: Option<Vec<usize>>,
    mut sync_rx: broadcast::Receiver<ReadWriteSyncMessage>,
    read_requests_concurrency: Arc<Semaphore>,
    requests_rx: Arc<Mutex<mpsc::Receiver<ReadRequest>>>,
    stored_confirmed_slot: StoredConfirmedSlot,
) -> anyhow::Result<thread::JoinHandle<anyhow::Result<()>>> {
    thread::Builder::new()
        .name(format!("alpStorageRd{index:02}"))
        .spawn(move || {
            tokio_uring::start(async move {
                if let Some(cpus) = affinity {
                    affinity::set_thread_affinity(&cpus).expect("failed to set affinity")
                }

                let (mut blocks, db_read, storage_files) = match sync_rx.recv().await {
                    Ok(ReadWriteSyncMessage::Init {
                        blocks,
                        db_read,
                        storage_files_init,
                    }) => {
                        let storage_files = StorageFilesRead::open(storage_files_init)
                            .await
                            .context("failed to open storage files")?;
                        (blocks, db_read, storage_files)
                    }
                    Ok(_) => anyhow::bail!("invalid sync message"),
                    Err(broadcast::error::RecvError::Closed) => return Ok(()), // shutdown
                    Err(broadcast::error::RecvError::Lagged(_)) => {
                        anyhow::bail!("read runtime lagged")
                    }
                };
                let mut confirmed_in_process = None;
                let mut storage_processed = StorageProcessed::default();
                let mut read_requests = FuturesUnordered::new();

                let result = start2(
                    index,
                    sync_rx,
                    &mut blocks,
                    &db_read,
                    &storage_files,
                    &mut confirmed_in_process,
                    &mut storage_processed,
                    read_requests_concurrency,
                    requests_rx,
                    &mut read_requests,
                    stored_confirmed_slot,
                )
                .await;

                loop {
                    match read_requests.next().await {
                        Some(Some(request)) => {
                            if let Some(future) = request.process(
                                &blocks,
                                &db_read,
                                &storage_files,
                                &confirmed_in_process,
                                &storage_processed,
                                None,
                            ) {
                                read_requests.push(future);
                            }
                        }
                        Some(None) => continue,
                        None => break,
                    }
                }

                result
            })
        })
        .map_err(Into::into)
}

#[allow(clippy::too_many_arguments)]
async fn start2(
    index: usize,
    mut sync_rx: broadcast::Receiver<ReadWriteSyncMessage>,
    blocks: &mut StoredBlocksRead,
    db_read: &RocksdbRead,
    storage_files: &StorageFilesRead,
    confirmed_in_process: &mut Option<(Slot, Option<Arc<BlockWithBinary>>)>,
    storage_processed: &mut StorageProcessed,
    read_requests_concurrency: Arc<Semaphore>,
    read_requests_rx: Arc<Mutex<mpsc::Receiver<ReadRequest>>>,
    read_requests: &mut FuturesUnordered<LocalBoxFuture<'_, Option<ReadRequest>>>,
    stored_confirmed_slot: StoredConfirmedSlot,
) -> anyhow::Result<()> {
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
            message = sync_rx.recv() => match message {
                Ok(ReadWriteSyncMessage::Init { .. }) => anyhow::bail!("unexpected second init"),
                Ok(ReadWriteSyncMessage::BlockNew { slot, block }) => storage_processed.add(slot, block),
                Ok(ReadWriteSyncMessage::BlockDead { slot }) => storage_processed.mark_dead(slot),
                Ok(ReadWriteSyncMessage::BlockConfirmed { slot, block }) => {
                    stored_confirmed_slot.set_confirmed(index, slot);
                    storage_processed.set_confirmed(slot, block.clone());
                    *confirmed_in_process = Some((slot, block));
                },
                Ok(ReadWriteSyncMessage::SlotFinalized { slot }) => {
                    storage_processed.set_finalized(slot);
                }
                Ok(ReadWriteSyncMessage::ConfirmedBlockPop) => blocks.pop_block(),
                Ok(ReadWriteSyncMessage::ConfirmedBlockPush { block }) => {
                    let Some((slot, _block)) = confirmed_in_process.take() else {
                        anyhow::bail!("expected confirmed before push");
                    };
                    anyhow::ensure!(slot == block.slot(), "unexpect confirmed block: {slot} vs {}", block.slot());
                    blocks.push_block(block);
                },
                Err(broadcast::error::RecvError::Closed) => return Ok(()), // shutdown
                Err(broadcast::error::RecvError::Lagged(_)) => anyhow::bail!("read runtime lagged"),
            },
            // existed request
            message = read_request_fut => match message {
                Some(Some(request)) => {
                    if let Some(future) = request.process(
                        blocks,
                        db_read,
                        storage_files,
                        confirmed_in_process,
                        storage_processed,
                        None
                    ) {
                        read_requests.push(future);
                    }
                }
                Some(None) => continue,
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

                if let Some(future) = request.process(
                    blocks,
                    db_read,
                    storage_files,
                    confirmed_in_process,
                    storage_processed,
                    Some(lock)
                ) {
                    read_requests.push(future);
                }
            },
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
struct SignatureStatus {
    slot: Slot,
    block_height: Slot,
    err: Option<TransactionError>,
}

#[derive(Debug)]
struct RecentBlock {
    blockhash: String,
    block_height: Slot,
    signatures: Vec<Signature>,
    fees: Arc<TransactionsFees>,
}

#[derive(Debug, Default)]
struct StorageProcessed {
    processed_slot: Slot,
    processed_height: Slot,
    confirmed_slot: Slot,
    confirmed_height: Slot,
    finalized_slot: Slot,
    finalized_height: Slot,
    blocks: BTreeMap<Slot, Option<Arc<BlockWithBinary>>>,
    signature_statuses: HashMap<Signature, SignatureStatus>,
    recent_blocks: BTreeMap<Slot, RecentBlock>,
    block_heights: HashMap<String, Slot>,
}

impl StorageProcessed {
    fn add_signatures(&mut self, slot: Slot, block: &BlockWithBinary) {
        if !self.blocks.contains_key(&slot) {
            if let Some(block_height) = block.block_height {
                for tx in block.transactions.values() {
                    self.signature_statuses.insert(
                        tx.signature,
                        SignatureStatus {
                            slot,
                            block_height,
                            err: tx.err.clone(),
                        },
                    );
                }
                self.recent_blocks.insert(
                    slot,
                    RecentBlock {
                        blockhash: block.blockhash.clone(),
                        block_height,
                        signatures: block.transactions.keys().copied().collect(),
                        fees: Arc::clone(&block.fees),
                    },
                );
                self.block_heights
                    .insert(block.blockhash.clone(), block_height);
            } else {
                error!(slot, "no block height for slot");
            }
        }
    }

    fn remove_signatures(&mut self, block: RecentBlock) {
        for signature in block.signatures {
            self.signature_statuses.remove(&signature);
        }
        self.block_heights.remove(&block.blockhash);
    }

    fn update_processed(&mut self) {
        let mut processed = None;
        for (slot, block) in self.blocks.iter().rev() {
            if let Some(block) = block {
                processed = Some((*slot, block.block_height));
                break;
            }
        }
        if let Some((slot, block_height)) = processed {
            self.processed_slot = self.processed_slot.max(slot);
            self.processed_height = self.processed_height.max(block_height.unwrap_or_default())
        } else {
            self.processed_slot = self.processed_slot.max(self.confirmed_slot);
            self.processed_height = self.processed_height.max(self.confirmed_height);
        }
    }

    fn add(&mut self, slot: Slot, block: Arc<BlockWithBinary>) {
        self.add_signatures(slot, &block);
        if let BTreeMapEntry::Vacant(entry) = self.blocks.entry(slot) {
            entry.insert(Some(block));
        }
        self.update_processed();
    }

    fn mark_dead(&mut self, slot: Slot) {
        if self.blocks.insert(slot, None).is_some() {
            if let Some(block) = self.recent_blocks.remove(&slot) {
                self.remove_signatures(block);
            }
        }
        self.update_processed();
    }

    fn set_confirmed(&mut self, slot: Slot, block: Option<Arc<BlockWithBinary>>) {
        self.confirmed_slot = slot;
        self.confirmed_height = self
            .recent_blocks
            .get(&slot)
            .map(|rb| rb.block_height)
            .unwrap_or(self.confirmed_height);

        if let Some(block) = &block {
            self.add_signatures(slot, block);
        }

        loop {
            match self.blocks.first_key_value() {
                Some((first_slot, _block)) if *first_slot <= slot => self.blocks.pop_first(),
                _ => break,
            };
        }
        self.update_processed();

        loop {
            match self.recent_blocks.first_key_value() {
                Some((_slot, block))
                    if (self.confirmed_height - block.block_height) as usize
                        >= MAX_RECENT_BLOCKHASHES =>
                {
                    if let Some((_slot, block)) = self.recent_blocks.pop_first() {
                        self.remove_signatures(block);
                    }
                }
                _ => break,
            }
        }
    }

    fn set_finalized(&mut self, slot: Slot) {
        self.finalized_slot = slot;
        self.finalized_height = self
            .recent_blocks
            .get(&slot)
            .map(|rb| rb.block_height)
            .unwrap_or(self.finalized_height);
    }

    fn get_processed_block(&self, slot: Slot) -> Option<&Arc<BlockWithBinary>> {
        self.blocks.get(&slot).and_then(|block| block.as_ref())
    }

    fn get_recent_block(&self, slot: Slot) -> Option<&RecentBlock> {
        self.recent_blocks.get(&slot)
    }

    fn get_signature_status(&self, signature: &Signature) -> Option<TransactionStatus> {
        self.signature_statuses.get(signature).map(|status| {
            let confirmations = if status.block_height > self.finalized_height {
                self.confirmed_height
                    .checked_sub(status.block_height)
                    .map(|x| (x + 1) as usize)
            } else {
                None
            };

            TransactionStatus {
                slot: status.slot,
                confirmations,
                status: match status.err.clone() {
                    Some(error) => Err(error),
                    None => Ok(()),
                },
                err: status.err.clone(),
                confirmation_status: Some(match confirmations {
                    Some(0) => TransactionConfirmationStatus::Processed,
                    Some(_) => TransactionConfirmationStatus::Confirmed,
                    None => TransactionConfirmationStatus::Finalized,
                }),
            }
        })
    }

    fn get_block_height_by_blockhash(&self, blockhash: &String) -> Option<Slot> {
        self.block_heights.get(blockhash).copied()
    }

    fn get_fees(&self, pubkeys: &[Pubkey], percentile: Option<u16>) -> Vec<RpcPrioritizationFee> {
        let mut fees = Vec::with_capacity(RPF_MAX_NUM_RECENT_BLOCKS);
        for (slot, block) in self.recent_blocks.iter().rev() {
            fees.push(RpcPrioritizationFee {
                slot: *slot,
                prioritization_fee: block.fees.get_fee(pubkeys, percentile),
            });
            if fees.len() == RPF_MAX_NUM_RECENT_BLOCKS {
                break;
            }
        }
        fees.reverse();
        fees
    }
}

#[derive(Debug)]
pub enum ReadResultBlock {
    Timeout,
    Removed,
    Dead,
    NotAvailable,
    Block(Vec<u8>),
    ReadError(io::Error),
}

#[derive(Debug)]
pub enum ReadResultBlockHeight {
    Timeout,
    BlockHeight(Slot),
    ReadError(anyhow::Error),
}

#[derive(Debug)]
pub enum ReadResultBlocks {
    Timeout,
    Blocks(Vec<Slot>),
    ReadError(anyhow::Error),
}

#[derive(Debug)]
pub enum ReadResultBlockTime {
    Timeout,
    Removed,
    Dead,
    NotAvailable,
    BlockTime(Option<UnixTimestamp>),
    ReadError(anyhow::Error),
}

#[derive(Debug)]
pub enum ReadResultLatestBlockhash {
    Timeout,
    LatestBlockhash {
        slot: Slot,
        blockhash: String,
        last_valid_block_height: Slot,
    },
    ReadError(anyhow::Error),
}

#[derive(Debug)]
pub enum ReadResultRecentPrioritizationFees {
    Timeout,
    Fees(Vec<RpcPrioritizationFee>),
}

#[derive(Debug)]
pub enum ReadResultSignaturesForAddress {
    Timeout,
    Signatures {
        signatures: Vec<RpcConfirmedTransactionStatusWithSignature>,
        finished: bool,
        before: Option<Signature>,
    },
    ReadError(anyhow::Error),
}

#[derive(Debug)]
pub enum ReadResultSignatureStatuses {
    Timeout,
    Signatures(Vec<Option<TransactionStatus>>),
    ReadError(anyhow::Error),
}

#[derive(Debug)]
pub enum ReadResultTransaction {
    Timeout,
    NotFound,
    Transaction {
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        bytes: Vec<u8>,
    },
    ReadError(anyhow::Error),
}

#[derive(Debug)]
pub enum ReadResultBlockhashValid {
    Timeout,
    Blockhash { slot: Slot, is_valid: bool },
    ReadError(anyhow::Error),
}

#[derive(Debug)]
pub enum ReadRequest {
    Block {
        deadline: Instant,
        slot: Slot,
        tx: oneshot::Sender<ReadResultBlock>,
    },
    BlockHeight {
        deadline: Instant,
        commitment: CommitmentConfig,
        tx: oneshot::Sender<ReadResultBlockHeight>,
    },
    Blocks {
        deadline: Instant,
        start_slot: Slot,
        until: RpcRequestBlocksUntil,
        commitment: CommitmentConfig,
        tx: oneshot::Sender<ReadResultBlocks>,
    },
    BlockTime {
        deadline: Instant,
        slot: Slot,
        tx: oneshot::Sender<ReadResultBlockTime>,
    },
    LatestBlockhash {
        deadline: Instant,
        commitment: CommitmentConfig,
        tx: oneshot::Sender<ReadResultLatestBlockhash>,
    },
    RecentPrioritizationFees {
        deadline: Instant,
        pubkeys: Vec<Pubkey>,
        percentile: Option<u16>,
        tx: oneshot::Sender<ReadResultRecentPrioritizationFees>,
    },
    SignaturesForAddress {
        deadline: Instant,
        commitment: CommitmentConfig,
        address: Pubkey,
        before: Option<Signature>,
        until: Option<Signature>,
        limit: usize,
        tx: oneshot::Sender<ReadResultSignaturesForAddress>,
    },
    SignaturesForAddress2 {
        deadline: Instant,
        address: Pubkey,
        slot: Slot,
        before: Option<Signature>,
        until: Signature,
        signatures: Vec<RpcConfirmedTransactionStatusWithSignature>,
        tx: oneshot::Sender<ReadResultSignaturesForAddress>,
        lock: Option<OwnedSemaphorePermit>,
    },
    SignaturesForAddress3 {
        deadline: Instant,
        signatures: Vec<RpcConfirmedTransactionStatusWithSignature>,
        finished: bool,
        tx: oneshot::Sender<ReadResultSignaturesForAddress>,
        lock: Option<OwnedSemaphorePermit>,
    },
    SignatureStatuses {
        deadline: Instant,
        signatures: Vec<Signature>,
        search_transaction_history: bool,
        tx: oneshot::Sender<ReadResultSignatureStatuses>,
    },
    Transaction {
        deadline: Instant,
        signature: Signature,
        tx: oneshot::Sender<ReadResultTransaction>,
    },
    Transaction2 {
        deadline: Instant,
        index: TransactionIndexValue<'static>,
        tx: oneshot::Sender<ReadResultTransaction>,
        lock: Option<OwnedSemaphorePermit>,
    },
    BlockhashValid {
        deadline: Instant,
        blockhash: String,
        commitment: CommitmentConfig,
        tx: oneshot::Sender<ReadResultBlockhashValid>,
    },
}

impl ReadRequest {
    fn process<'a>(
        self,
        blocks: &StoredBlocksRead,
        db_read: &RocksdbRead,
        storage_files: &StorageFilesRead,
        confirmed_in_process: &Option<(Slot, Option<Arc<BlockWithBinary>>)>,
        storage_processed: &StorageProcessed,
        lock: Option<OwnedSemaphorePermit>,
    ) -> Option<LocalBoxFuture<'a, Option<Self>>> {
        match self {
            Self::Block { deadline, slot, tx } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultBlock::Timeout);
                    return None;
                }

                if let Some((confirmed_in_process_slot, confirmed_in_process_block)) =
                    confirmed_in_process
                {
                    if *confirmed_in_process_slot == slot {
                        let _ = tx.send(if let Some(block) = confirmed_in_process_block {
                            ReadResultBlock::Block(block.protobuf.clone())
                        } else {
                            ReadResultBlock::Dead
                        });
                        return None;
                    }
                }

                let location = match blocks.get_block_location(slot) {
                    StorageBlockLocationResult::Removed => {
                        let _ = tx.send(ReadResultBlock::Removed);
                        return None;
                    }
                    StorageBlockLocationResult::Dead => {
                        let _ = tx.send(ReadResultBlock::Dead);
                        return None;
                    }
                    StorageBlockLocationResult::NotAvailable => {
                        let _ = tx.send(ReadResultBlock::NotAvailable);
                        return None;
                    }
                    StorageBlockLocationResult::SlotMismatch => {
                        error!(slot, "item/slot mismatch");
                        let _ = tx.send(ReadResultBlock::ReadError(io::Error::new(
                            io::ErrorKind::Other,
                            "item/slot mismatch",
                        )));
                        return None;
                    }
                    StorageBlockLocationResult::Found(location) => location,
                };

                let read_fut =
                    storage_files.read(location.storage_id, location.offset, location.size);

                Some(Box::pin(async move {
                    let result = match timeout_at(deadline.into(), read_fut).await {
                        Ok(Ok(bytes)) => ReadResultBlock::Block(bytes),
                        Ok(Err(error)) => ReadResultBlock::ReadError(error),
                        Err(_error) => ReadResultBlock::Timeout,
                    };
                    let _ = tx.send(result);
                    drop(lock);
                    None
                }))
            }
            Self::BlockHeight {
                deadline,
                commitment,
                tx,
            } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultBlockHeight::Timeout);
                    return None;
                }

                let mut block_height = None;
                let mut commitment_slot = None;

                if commitment.is_processed() {
                    block_height = Some(storage_processed.processed_height);
                }

                if commitment.is_confirmed() {
                    if let Some((_confirmed_in_process_slot, Some(confirmed_in_process_block))) =
                        confirmed_in_process
                    {
                        block_height = confirmed_in_process_block.block_height;
                    }

                    if block_height.is_none() {
                        commitment_slot = Some(storage_processed.confirmed_slot);
                    }
                }

                if commitment.is_finalized() {
                    commitment_slot = Some(storage_processed.finalized_slot);
                }

                if let Some(mut slot) = commitment_slot {
                    while block_height.is_none() {
                        match blocks.get_block_location(slot) {
                            StorageBlockLocationResult::Dead => {
                                slot -= 1;
                            }
                            StorageBlockLocationResult::SlotMismatch => {
                                error!(slot = slot, "item/slot mismatch");
                                let _ = tx.send(ReadResultBlockHeight::ReadError(anyhow::anyhow!(
                                    io::Error::new(io::ErrorKind::Other, "item/slot mismatch",)
                                )));
                                return None;
                            }
                            StorageBlockLocationResult::Found(location) => {
                                block_height = location.block_height;
                            }
                            StorageBlockLocationResult::Removed
                            | StorageBlockLocationResult::NotAvailable => {
                                let _ = tx.send(ReadResultBlockHeight::ReadError(anyhow::anyhow!(
                                    "failed to find commitment slot"
                                )));
                                return None;
                            }
                        };
                    }
                }

                let _ = tx.send(
                    block_height
                        .map(ReadResultBlockHeight::BlockHeight)
                        .unwrap_or_else(|| {
                            ReadResultBlockHeight::ReadError(anyhow::anyhow!(
                                "failed to get block height"
                            ))
                        }),
                );
                None
            }
            Self::Blocks {
                deadline,
                start_slot,
                until,
                commitment,
                tx,
            } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultBlocks::Timeout);
                    return None;
                }

                let result = match blocks.get_blocks(
                    start_slot,
                    if commitment.is_confirmed() {
                        storage_processed.confirmed_slot
                    } else {
                        storage_processed.finalized_slot
                    },
                    until,
                ) {
                    Ok(blocks) => ReadResultBlocks::Blocks(blocks),
                    Err(error) => ReadResultBlocks::ReadError(error),
                };

                let _ = tx.send(result);
                None
            }
            Self::BlockTime { deadline, slot, tx } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultBlockTime::Timeout);
                    return None;
                }

                let result = if slot <= storage_processed.confirmed_slot {
                    match blocks.get_block_location(slot) {
                        StorageBlockLocationResult::Removed => ReadResultBlockTime::Removed,
                        StorageBlockLocationResult::Dead => ReadResultBlockTime::Dead,
                        StorageBlockLocationResult::SlotMismatch => {
                            error!(slot = slot, "item/slot mismatch");
                            ReadResultBlockTime::ReadError(anyhow::anyhow!(io::Error::new(
                                io::ErrorKind::Other,
                                "item/slot mismatch",
                            )))
                        }
                        StorageBlockLocationResult::Found(location) => {
                            ReadResultBlockTime::BlockTime(location.block_time)
                        }
                        StorageBlockLocationResult::NotAvailable => {
                            ReadResultBlockTime::ReadError(anyhow::anyhow!("failed to find slot"))
                        }
                    }
                } else {
                    match storage_processed.get_processed_block(slot) {
                        Some(block) => ReadResultBlockTime::BlockTime(block.block_time),
                        _ => ReadResultBlockTime::NotAvailable,
                    }
                };

                let _ = tx.send(result);
                None
            }
            Self::LatestBlockhash {
                deadline,
                commitment,
                tx,
            } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultLatestBlockhash::Timeout);
                    return None;
                }

                let slot = match commitment.commitment {
                    CommitmentLevel::Processed => storage_processed.processed_slot,
                    CommitmentLevel::Confirmed => storage_processed.confirmed_slot,
                    CommitmentLevel::Finalized => storage_processed.finalized_slot,
                };
                let result = match storage_processed.get_recent_block(slot) {
                    Some(block) => ReadResultLatestBlockhash::LatestBlockhash {
                        slot,
                        blockhash: block.blockhash.clone(),
                        last_valid_block_height: block.block_height + MAX_PROCESSING_AGE as u64,
                    },
                    None => {
                        ReadResultLatestBlockhash::ReadError(anyhow::anyhow!("failed to get block"))
                    }
                };

                let _ = tx.send(result);
                None
            }
            Self::RecentPrioritizationFees {
                deadline,
                pubkeys,
                percentile,
                tx,
            } => {
                let result = if deadline < Instant::now() {
                    ReadResultRecentPrioritizationFees::Timeout
                } else {
                    ReadResultRecentPrioritizationFees::Fees(
                        storage_processed.get_fees(&pubkeys, percentile),
                    )
                };

                let _ = tx.send(result);
                None
            }
            Self::SignaturesForAddress {
                deadline,
                commitment,
                address,
                mut before,
                until,
                limit,
                tx,
            } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultSignaturesForAddress::Timeout);
                    return None;
                }

                let until = until.unwrap_or_default();
                let mut signatures = Vec::with_capacity(limit);

                // try to get from current processing confirmed block
                let highest_slot = if commitment.is_confirmed() {
                    if let Some((confirmed_in_process_slot, Some(block))) = confirmed_in_process {
                        if let Some(sfa) = block.sfa.get(&address) {
                            let skip_count = match before {
                                Some(sig) => {
                                    if let Some(index) =
                                        sfa.signatures.iter().position(|sfa| sfa.signature == sig)
                                    {
                                        before = None; // reset before because we found it
                                        index + 1
                                    } else {
                                        sfa.signatures.len() // skip signatures if before not found
                                    }
                                }
                                None => 0, // add all signatures if no before arg
                            };

                            let mut finished = false;
                            for item in sfa.signatures.iter().skip(skip_count) {
                                if item.signature == until {
                                    finished = true;
                                    break;
                                }

                                signatures.push(RpcConfirmedTransactionStatusWithSignature {
                                    signature: item.signature.to_string(),
                                    slot: *confirmed_in_process_slot,
                                    err: item.err.clone(),
                                    memo: item.memo.clone(),
                                    block_time: block.block_time,
                                    confirmation_status: Some(
                                        TransactionConfirmationStatus::Confirmed,
                                    ),
                                });

                                if signatures.len() == signatures.capacity() {
                                    finished = true;
                                    break;
                                }
                            }
                            if finished {
                                let _ = tx.send(ReadResultSignaturesForAddress::Signatures {
                                    signatures,
                                    finished: true,
                                    before: None,
                                });
                                return None;
                            }
                        }
                    }
                    storage_processed.confirmed_slot
                } else {
                    storage_processed.finalized_slot
                };

                if let Some(before) = before {
                    // read slot for before signature
                    let read_tx_index = match db_read.read_tx_index(before) {
                        Ok(fut) => fut,
                        Err(error) => {
                            let _ = tx.send(ReadResultSignaturesForAddress::ReadError(error));
                            return None;
                        }
                    };

                    Some(Box::pin(async move {
                        let result = match read_tx_index.await {
                            Ok(Some(index)) if index.slot <= highest_slot => {
                                return Some(ReadRequest::SignaturesForAddress2 {
                                    deadline,
                                    address,
                                    slot: index.slot,
                                    before: Some(before),
                                    until,
                                    signatures,
                                    tx,
                                    lock,
                                });
                            }
                            // found but not satisfy commitment, return empty vec
                            Ok(Some(_index)) => ReadResultSignaturesForAddress::Signatures {
                                signatures: vec![],
                                finished: true,
                                before: None,
                            },
                            // not found, maybe upstream storage have an index
                            Ok(None) => ReadResultSignaturesForAddress::Signatures {
                                signatures: vec![],
                                finished: false,
                                before: Some(before),
                            },
                            Err(error) => ReadResultSignaturesForAddress::ReadError(error),
                        };

                        let _ = tx.send(result);
                        None
                    }))
                } else {
                    Some(Box::pin(ready(Some(ReadRequest::SignaturesForAddress2 {
                        deadline,
                        address,
                        slot: highest_slot,
                        before: None,
                        until,
                        signatures,
                        tx,
                        lock,
                    }))))
                }
            }
            Self::SignaturesForAddress2 {
                deadline,
                address,
                slot,
                before,
                until,
                signatures,
                tx,
                lock,
            } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultSignaturesForAddress::Timeout);
                    return None;
                }

                let read_sigs_index = match db_read
                    .read_signatures_for_address(address, slot, before, until, signatures)
                {
                    Ok(fut) => fut,
                    Err(error) => {
                        let _ = tx.send(ReadResultSignaturesForAddress::ReadError(error));
                        return None;
                    }
                };

                Some(Box::pin(async move {
                    match read_sigs_index.await {
                        Ok((signatures, finished)) => Some(ReadRequest::SignaturesForAddress3 {
                            deadline,
                            signatures,
                            finished,
                            tx,
                            lock,
                        }),
                        Err(error) => {
                            let _ = tx.send(ReadResultSignaturesForAddress::ReadError(error));
                            None
                        }
                    }
                }))
            }
            Self::SignaturesForAddress3 {
                deadline,
                signatures,
                mut finished,
                tx,
                lock,
            } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultSignaturesForAddress::Timeout);
                    return None;
                }

                let result = match signatures
                    .into_iter()
                    .filter_map(|mut sig| match blocks.get_block_location(sig.slot) {
                        StorageBlockLocationResult::SlotMismatch => {
                            error!(slot = sig.slot, "item/slot mismatch");
                            Some(Err(ReadResultSignaturesForAddress::ReadError(
                                anyhow::anyhow!(io::Error::new(
                                    io::ErrorKind::Other,
                                    "item/slot mismatch",
                                )),
                            )))
                        }
                        StorageBlockLocationResult::Found(location) => {
                            sig.block_time = location.block_time;
                            sig.confirmation_status =
                                Some(if sig.slot <= storage_processed.finalized_slot {
                                    TransactionConfirmationStatus::Finalized
                                } else {
                                    TransactionConfirmationStatus::Confirmed
                                });
                            Some(Ok(sig))
                        }
                        _ => {
                            finished = false;
                            None
                        }
                    })
                    .collect::<Result<Vec<_>, _>>()
                    .map(|signatures| ReadResultSignaturesForAddress::Signatures {
                        signatures,
                        finished,
                        before: None,
                    }) {
                    Ok(value) => value,
                    Err(value) => value,
                };

                let _ = tx.send(result);
                drop(lock);
                None
            }
            Self::SignatureStatuses {
                deadline,
                signatures,
                search_transaction_history,
                tx,
            } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultSignatureStatuses::Timeout);
                    return None;
                }

                let mut signatures_found =
                    HashMap::with_capacity_and_hasher(signatures.len(), Default::default());
                let mut signatures_history = Vec::with_capacity(if search_transaction_history {
                    signatures.len()
                } else {
                    0
                });
                for signature in signatures.iter().copied() {
                    if let Some(status) = storage_processed.get_signature_status(&signature) {
                        signatures_found.insert(signature, status.clone());
                        continue;
                    }

                    if search_transaction_history {
                        signatures_history.push(signature);
                    }
                }

                fn create_result(
                    signatures: &[Signature],
                    mut found: HashMap<Signature, TransactionStatus>,
                ) -> ReadResultSignatureStatuses {
                    ReadResultSignatureStatuses::Signatures(
                        signatures
                            .iter()
                            .map(|signature| found.remove(signature))
                            .collect(),
                    )
                }

                if signatures_history.is_empty() {
                    let _ = tx.send(create_result(&signatures, signatures_found));
                    return None;
                }

                let read_fut = match db_read.read_signature_statuses(signatures_history) {
                    Ok(fut) => fut,
                    Err(error) => {
                        let _ = tx.send(ReadResultSignatureStatuses::ReadError(error));
                        return None;
                    }
                };

                let finalized_slot = storage_processed.finalized_slot;
                Some(Box::pin(async move {
                    let result = match read_fut.await {
                        Ok(signatures_history) => {
                            for (signature, value) in signatures_history {
                                if value.slot <= finalized_slot {
                                    signatures_found.insert(
                                        signature,
                                        TransactionStatus {
                                            slot: value.slot,
                                            confirmations: None,
                                            status: match value.err.clone() {
                                                Some(error) => Err(error.into_owned()),
                                                None => Ok(()),
                                            },
                                            err: value.err.map(|x| x.into_owned()),
                                            confirmation_status: Some(
                                                TransactionConfirmationStatus::Finalized,
                                            ),
                                        },
                                    );
                                }
                            }
                            create_result(&signatures, signatures_found)
                        }
                        Err(error) => ReadResultSignatureStatuses::ReadError(error),
                    };

                    let _ = tx.send(result);
                    drop(lock);
                    None
                }))
            }
            Self::Transaction {
                deadline,
                signature,
                tx,
            } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultTransaction::Timeout);
                    return None;
                }

                if let Some((confirmed_in_process_slot, Some(block))) = confirmed_in_process {
                    if let Some(transaction) = block.transactions.get(&signature) {
                        let _ = tx.send(ReadResultTransaction::Transaction {
                            slot: *confirmed_in_process_slot,
                            block_time: block.block_time,
                            bytes: transaction.protobuf.clone(),
                        });
                        return None;
                    }
                }

                let read_fut = match db_read.read_tx_index(signature) {
                    Ok(fut) => fut,
                    Err(error) => {
                        let _ = tx.send(ReadResultTransaction::ReadError(error));
                        return None;
                    }
                };

                Some(Box::pin(async move {
                    let result = match read_fut.await {
                        Ok(Some(index)) => {
                            return Some(ReadRequest::Transaction2 {
                                deadline,
                                index,
                                tx,
                                lock,
                            });
                        }
                        Ok(None) => ReadResultTransaction::NotFound,
                        Err(error) => ReadResultTransaction::ReadError(error),
                    };

                    let _ = tx.send(result);
                    None
                }))
            }
            Self::Transaction2 {
                deadline,
                index,
                tx,
                lock,
            } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultTransaction::Timeout);
                    return None;
                }

                let location = match blocks.get_block_location(index.slot) {
                    StorageBlockLocationResult::SlotMismatch => {
                        error!(slot = index.slot, "item/slot mismatch");
                        let _ = tx.send(ReadResultTransaction::ReadError(anyhow::anyhow!(
                            io::Error::new(io::ErrorKind::Other, "item/slot mismatch",)
                        )));
                        return None;
                    }
                    StorageBlockLocationResult::Found(location) => location,
                    StorageBlockLocationResult::Removed
                    | StorageBlockLocationResult::Dead
                    | StorageBlockLocationResult::NotAvailable => {
                        let _ = tx.send(ReadResultTransaction::NotFound);
                        return None;
                    }
                };

                let read_fut = storage_files.read(
                    location.storage_id,
                    location.offset + index.offset,
                    index.size,
                );

                Some(Box::pin(async move {
                    let result = match timeout_at(deadline.into(), read_fut).await {
                        Ok(Ok(bytes)) => ReadResultTransaction::Transaction {
                            slot: index.slot,
                            block_time: location.block_time,
                            bytes,
                        },
                        Ok(Err(error)) => {
                            ReadResultTransaction::ReadError(anyhow::Error::new(error))
                        }
                        Err(_error) => ReadResultTransaction::Timeout,
                    };
                    let _ = tx.send(result);
                    drop(lock);
                    None
                }))
            }
            Self::BlockhashValid {
                deadline,
                blockhash,
                commitment,
                tx,
            } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultBlockhashValid::Timeout);
                    return None;
                }

                let commitment_slot = match commitment.commitment {
                    CommitmentLevel::Processed => storage_processed.processed_slot,
                    CommitmentLevel::Confirmed => storage_processed.confirmed_slot,
                    CommitmentLevel::Finalized => storage_processed.finalized_slot,
                };

                let commitment_height = match commitment.commitment {
                    CommitmentLevel::Processed => storage_processed.processed_height,
                    CommitmentLevel::Confirmed => storage_processed.confirmed_height,
                    CommitmentLevel::Finalized => storage_processed.finalized_height,
                };

                let Some(block_height) =
                    storage_processed.get_block_height_by_blockhash(&blockhash)
                else {
                    let _ = tx.send(ReadResultBlockhashValid::ReadError(anyhow::anyhow!(
                        "failed to get block height"
                    )));
                    return None;
                };

                let _ = tx.send(ReadResultBlockhashValid::Blockhash {
                    slot: commitment_slot,
                    is_valid: block_height + (MAX_PROCESSING_AGE as u64) > commitment_height,
                });
                None
            }
        }
    }
}
