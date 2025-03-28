use {
    crate::{
        config::ConfigStorageRocksdb,
        source::block::BlockWithBinary,
        storage::{
            blocks::{StoredBlock, StoredBlocksWrite},
            files::{StorageFilesWrite, StorageId},
            sync::ReadWriteSyncMessage,
        },
    },
    anyhow::Context,
    bitflags::bitflags,
    foldhash::quality::SeedableRandomState,
    futures::future::BoxFuture,
    prost::{
        bytes::Buf,
        encoding::{decode_varint, encode_varint},
    },
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, IteratorMode, Options,
        WriteBatch,
    },
    solana_sdk::{
        clock::{Slot, UnixTimestamp},
        signature::Signature,
    },
    std::{
        hash::BuildHasher,
        sync::{Arc, Mutex, mpsc},
        thread::{Builder, JoinHandle},
    },
    tokio::sync::{broadcast, oneshot},
};

trait ColumnName {
    const NAME: &'static str;
}

#[derive(Debug)]
pub struct SlotIndex;

impl ColumnName for SlotIndex {
    const NAME: &'static str = "slot_index";
}

impl SlotIndex {
    pub fn key(slot: Slot) -> [u8; 8] {
        slot.to_be_bytes()
    }

    pub fn decode(slice: &[u8]) -> anyhow::Result<Slot> {
        slice
            .try_into()
            .map(Slot::from_be_bytes)
            .context("invalid slice size")
    }
}

#[derive(Debug, Default, Clone)]
pub struct SlotIndexValue {
    pub dead: bool,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<Slot>,
    pub storage_id: StorageId,
    pub offset: u64,
    pub size: u64,
    pub transactions: Vec<[u8; 8]>,
}

impl SlotIndexValue {
    fn encode(&self, buf: &mut Vec<u8>) {
        if self.dead {
            encode_varint(SlotIndexValueFlags::DEAD.bits() as u64, buf);
            return;
        }

        let mut flags = SlotIndexValueFlags::empty();
        if self.dead {
            flags |= SlotIndexValueFlags::DEAD;
        }
        if self.block_time.is_some() {
            flags |= SlotIndexValueFlags::BLOCK_TIME;
        }
        if self.block_height.is_some() {
            flags |= SlotIndexValueFlags::BLOCK_HEIGHT;
        }

        encode_varint(flags.bits() as u64, buf);
        if let Some(block_time) = self.block_time {
            encode_varint(block_time as u64, buf);
        }
        if let Some(block_height) = self.block_height {
            encode_varint(block_height, buf);
        }
        encode_varint(self.storage_id as u64, buf);
        encode_varint(self.offset, buf);
        encode_varint(self.size, buf);
        for hash in self.transactions.iter() {
            buf.extend_from_slice(hash);
        }
    }

    fn decode(mut slice: &[u8], decode_transactions: bool) -> anyhow::Result<Self> {
        let flags =
            SlotIndexValueFlags::from_bits(slice.try_get_u8().context("failed to read flags")?)
                .context("invalid flags")?;

        if flags.contains(SlotIndexValueFlags::DEAD) {
            return Ok(Self {
                dead: true,
                ..Default::default()
            });
        }

        Ok(Self {
            dead: false,
            block_time: flags
                .contains(SlotIndexValueFlags::BLOCK_TIME)
                .then(|| decode_varint(&mut slice).map(|bt| bt as i64))
                .transpose()
                .context("failed to read block time")?,
            block_height: flags
                .contains(SlotIndexValueFlags::BLOCK_HEIGHT)
                .then(|| decode_varint(&mut slice))
                .transpose()
                .context("failed to read block_height")?,
            storage_id: decode_varint(&mut slice)
                .context("failed to read storage id")?
                .try_into()
                .context("failed to convert storage id")?,
            offset: decode_varint(&mut slice).context("failed to read offset")?,
            size: decode_varint(&mut slice).context("failed to read size")?,
            transactions: {
                anyhow::ensure!(slice.len() % 8 == 0, "invalid size of transactions");
                if decode_transactions {
                    let mut transactions = Vec::with_capacity(slice.len() / 8);
                    for i in 0..slice.len() / 8 {
                        let hash = slice[i * 8..(i + 1) * 8].try_into().expect("valid slice");
                        transactions.push(hash);
                    }
                    transactions
                } else {
                    vec![]
                }
            },
        })
    }
}

bitflags! {
    struct SlotIndexValueFlags: u8 {
        const DEAD =         0b00000001;
        const BLOCK_TIME =   0b00000010;
        const BLOCK_HEIGHT = 0b00000100;
    }
}

#[derive(Debug)]
pub struct TransactionIndex;

impl ColumnName for TransactionIndex {
    const NAME: &'static str = "tx_index";
}

impl TransactionIndex {
    pub fn key(signature: &Signature) -> [u8; 8] {
        thread_local! {
            static HASHER: SeedableRandomState = SeedableRandomState::fixed();
        }

        let hash = HASHER.with(|hasher| hasher.hash_one(signature));
        hash.to_be_bytes()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct TransactionIndexValue {
    pub slot: Slot,
    pub offset: u64,
    pub size: u64,
}

impl TransactionIndexValue {
    fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint(self.slot, buf);
        encode_varint(self.offset, buf);
        encode_varint(self.size, buf);
    }

    fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {
        Ok(Self {
            slot: decode_varint(&mut slice).context("failed to decode slot")?,
            offset: decode_varint(&mut slice).context("failed to decode offset")?,
            size: decode_varint(&mut slice).context("failed to decode size")?,
        })
    }
}

#[derive(Debug, Clone)]
pub struct Rocksdb;

impl Rocksdb {
    #[allow(clippy::type_complexity)]
    pub fn open(
        config: ConfigStorageRocksdb,
        sync_tx: broadcast::Sender<ReadWriteSyncMessage>,
    ) -> anyhow::Result<(
        RocksdbWrite,
        RocksdbRead,
        Vec<(String, Option<JoinHandle<anyhow::Result<()>>>)>,
    )> {
        let db_options = Self::get_db_options();
        let cf_descriptors = Self::cf_descriptors();

        let db = Arc::new(
            DB::open_cf_descriptors(&db_options, &config.path, cf_descriptors)
                .with_context(|| format!("failed to open rocksdb with path: {:?}", config.path))?,
        );

        let (write_tx, write_rx) = mpsc::sync_channel(1);
        let (read_tx, read_rx) = mpsc::sync_channel(config.read_channel_size);

        let mut threads = vec![];
        let jh = Builder::new().name("rocksdbWrt".to_owned()).spawn({
            let db = Arc::clone(&db);
            move || {
                RocksdbWrite::spawn(db, write_rx);
                Ok(())
            }
        })?;
        threads.push(("rocksdbWrt".to_owned(), Some(jh)));
        let read_rx = Arc::new(Mutex::new(read_rx));
        for index in 0..config.read_workers {
            let th_name = format!("rocksdbRd{index:02}");
            let jh = Builder::new().name(th_name.clone()).spawn({
                let db = Arc::clone(&db);
                let read_rx = Arc::clone(&read_rx);
                move || {
                    RocksdbRead::spawn(db, read_rx);
                    Ok(())
                }
            })?;
            threads.push((th_name, Some(jh)));
        }

        Ok((
            RocksdbWrite {
                req_tx: write_tx,
                sync_tx,
            },
            RocksdbRead { req_tx: read_tx },
            threads,
        ))
    }

    fn get_db_options() -> Options {
        let mut options = Options::default();

        // Create if not exists
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Per the docs, a good value for this is the number of cores on the machine
        options.increase_parallelism(num_cpus::get() as i32);

        // While a compaction is ongoing, all the background threads
        // could be used by the compaction. This can stall writes which
        // need to flush the memtable. Add some high-priority background threads
        // which can service these writes.
        let mut env = rocksdb::Env::new().unwrap();
        env.set_high_priority_background_threads(4);
        options.set_env(&env);

        // Set max total WAL size
        options.set_max_total_wal_size(512 * 1024 * 1024);

        options
    }

    fn get_cf_options() -> Options {
        let mut options = Options::default();

        const MAX_WRITE_BUFFER_SIZE: u64 = 256 * 1024 * 1024;
        options.set_max_write_buffer_number(2);
        options.set_write_buffer_size(MAX_WRITE_BUFFER_SIZE as usize);

        let file_num_compaction_trigger = 4;
        let total_size_base = MAX_WRITE_BUFFER_SIZE * file_num_compaction_trigger;
        let file_size_base = total_size_base / 10;
        options.set_level_zero_file_num_compaction_trigger(file_num_compaction_trigger as i32);
        options.set_max_bytes_for_level_base(total_size_base);
        options.set_target_file_size_base(file_size_base);

        options.set_compression_type(DBCompressionType::None);

        options
    }

    fn cf_descriptors() -> Vec<ColumnFamilyDescriptor> {
        vec![
            Self::cf_descriptor::<SlotIndex>(),
            Self::cf_descriptor::<TransactionIndex>(),
        ]
    }

    fn cf_descriptor<C: ColumnName>() -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options())
    }

    fn cf_handle<C: ColumnName>(db: &DB) -> &ColumnFamily {
        db.cf_handle(C::NAME)
            .expect("should never get an unknown column")
    }
}

#[derive(Debug)]
enum WriteRequest {
    Transactions {
        slot: Slot,
        block: Arc<BlockWithBinary>,
        tx: oneshot::Sender<anyhow::Result<()>>,
    },
    SlotAdd {
        slot: Slot,
        data: Option<(Arc<BlockWithBinary>, StorageId, u64)>,
        tx: oneshot::Sender<anyhow::Result<()>>,
    },
    SlotRemove {
        slot: Slot,
        tx: oneshot::Sender<anyhow::Result<()>>,
    },
}

#[derive(Debug)]
pub struct RocksdbWrite {
    req_tx: mpsc::SyncSender<WriteRequest>,
    sync_tx: broadcast::Sender<ReadWriteSyncMessage>,
}

impl RocksdbWrite {
    fn spawn(db: Arc<DB>, write_rx: mpsc::Receiver<WriteRequest>) {
        let mut buf = vec![];

        loop {
            let Ok(request) = write_rx.recv() else {
                break;
            };

            match request {
                WriteRequest::Transactions { slot, block, tx } => {
                    let mut batch = WriteBatch::with_capacity_bytes(256 * 1024); // 256KiB
                    for tx_offset in block.txs_offset.iter() {
                        buf.clear();
                        TransactionIndexValue {
                            slot,
                            offset: tx_offset.offset,
                            size: tx_offset.size,
                        }
                        .encode(&mut buf);
                        batch.put_cf(
                            Rocksdb::cf_handle::<TransactionIndex>(&db),
                            tx_offset.hash,
                            &buf,
                        );
                    }
                    if tx.send(db.write(batch).map_err(Into::into)).is_err() {
                        break;
                    }
                }
                WriteRequest::SlotAdd { slot, data, tx } => {
                    let mut batch = WriteBatch::with_capacity_bytes(32 * 1024); // 32KiB

                    let block = if let Some((block, storage_id, offset)) = data {
                        SlotIndexValue {
                            dead: false,
                            block_time: block.block_time,
                            block_height: block.block_height,
                            storage_id,
                            offset,
                            size: block.protobuf.len() as u64,
                            transactions: block.txs_offset.iter().map(|txo| txo.hash).collect(),
                        }
                    } else {
                        SlotIndexValue {
                            dead: true,
                            ..Default::default()
                        }
                    };
                    buf.clear();
                    block.encode(&mut buf);
                    batch.put_cf(
                        Rocksdb::cf_handle::<SlotIndex>(&db),
                        SlotIndex::key(slot),
                        &buf,
                    );

                    if tx.send(db.write(batch).map_err(Into::into)).is_err() {
                        break;
                    }
                }
                WriteRequest::SlotRemove { slot, tx } => {
                    if tx.send(Self::spawn_remove_slot(&db, slot)).is_err() {
                        break;
                    }
                }
            }
        }
    }

    fn spawn_remove_slot(db: &DB, slot: Slot) -> anyhow::Result<()> {
        let value = db
            .get_pinned_cf(Rocksdb::cf_handle::<SlotIndex>(db), SlotIndex::key(slot))
            .context("failed to get existed slot")?
            .ok_or_else(|| anyhow::anyhow!("existed slot {slot} not found"))?;
        let value = SlotIndexValue::decode(&value, true).context("failed to decode slot data")?;

        let mut batch = WriteBatch::with_capacity_bytes(32 * 1024); // 32KiB
        batch.delete_cf(Rocksdb::cf_handle::<SlotIndex>(db), SlotIndex::key(slot));
        for tx in value.transactions {
            batch.delete_cf(Rocksdb::cf_handle::<TransactionIndex>(db), tx);
        }
        db.write(batch).map_err(Into::into)
    }

    pub async fn push_block(
        &self,
        slot: Slot,
        block: Option<Arc<BlockWithBinary>>,
        files: &mut StorageFilesWrite,
        blocks: &mut StoredBlocksWrite,
    ) -> anyhow::Result<()> {
        // get some space if we reached blocks limit
        if blocks.is_full() {
            self.pop_block(files, blocks).await?;
        }

        // store dead slot
        let Some(block) = block else {
            // db
            let (tx, rx) = oneshot::channel();
            self.req_tx
                .send(WriteRequest::SlotAdd {
                    slot,
                    data: None,
                    tx,
                })
                .context("failed to send WriteRequest::SlotAdd request")?;
            rx.await
                .context("failed to get WriteRequest::SlotAdd request result")??;

            // blocks
            blocks.push_block_dead(slot)?;

            return Ok(());
        };

        // 1) store block in file
        // 2) tx-index in db
        let mut buffer = block.protobuf.clone();
        let ((storage_id, offset, buffer, blocks), block) = tokio::try_join!(
            async move {
                loop {
                    let (buffer2, result) = files.push_block(buffer).await?;
                    buffer = buffer2;

                    if let Some((storage_id, offset)) = result {
                        return Ok((storage_id, offset, buffer, blocks));
                    }

                    self.pop_block(files, blocks).await?;
                }
            },
            async move {
                let (tx, rx) = oneshot::channel();
                self.req_tx
                    .send(WriteRequest::Transactions {
                        slot,
                        block: Arc::clone(&block),
                        tx,
                    })
                    .context("failed to send WriteRequest::Transactions request")?;
                rx.await
                    .context("failed to get WriteRequest::Transactions request result")??;
                Ok::<_, anyhow::Error>(block)
            }
        )?;

        let block_time = block.block_time;
        let block_height = block.block_height;
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(WriteRequest::SlotAdd {
                slot,
                data: Some((block, storage_id, offset)),
                tx,
            })
            .context("failed to send WriteRequest::SlotAdd request")?;
        rx.await
            .context("failed to get WriteRequest::SlotAdd request result")??;

        blocks.push_block_confirmed(
            slot,
            block_time,
            block_height,
            storage_id,
            offset,
            buffer.len() as u64,
        )
    }

    async fn pop_block(
        &self,
        files: &mut StorageFilesWrite,
        blocks: &mut StoredBlocksWrite,
    ) -> anyhow::Result<()> {
        let Some(block) = blocks.pop_block() else {
            anyhow::bail!("no blocks to remove");
        };
        let _ = self.sync_tx.send(ReadWriteSyncMessage::ConfirmedBlockPop);

        // remove from db
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(WriteRequest::SlotRemove {
                slot: block.slot,
                tx,
            })
            .context("failed to send WriteRequest::SlotRemove request")?;
        rx.await
            .context("failed to get WriteRequest::SlotRemove request result")??;

        // update offset in file
        if block.size == 0 {
            Ok(())
        } else {
            files.pop_block(block)
        }
    }
}

#[derive(Debug)]
enum ReadRequest {
    Slots {
        tx: oneshot::Sender<anyhow::Result<Vec<StoredBlock>>>,
    },
    Transaction {
        signature: Signature,
        tx: oneshot::Sender<anyhow::Result<Option<TransactionIndexValue>>>,
    },
}

#[derive(Debug, Clone)]
pub struct RocksdbRead {
    req_tx: mpsc::SyncSender<ReadRequest>,
}

impl RocksdbRead {
    fn spawn(db: Arc<DB>, read_rx: Arc<Mutex<mpsc::Receiver<ReadRequest>>>) {
        loop {
            let lock = read_rx.lock().expect("unpanicked mutex");
            let Ok(request) = lock.recv() else {
                break;
            };
            drop(lock);

            match request {
                ReadRequest::Slots { tx } => {
                    if tx.send(Self::spawn_slots(&db)).is_err() {
                        break;
                    }
                }
                ReadRequest::Transaction { signature, tx } => {
                    let result = match db.get_pinned_cf(
                        Rocksdb::cf_handle::<TransactionIndex>(&db),
                        TransactionIndex::key(&signature),
                    ) {
                        Ok(Some(slice)) => TransactionIndexValue::decode(slice.as_ref()).map(Some),
                        Ok(None) => Ok(None),
                        Err(error) => Err(anyhow::anyhow!("failed to get tx location: {error:?}")),
                    };

                    if tx.send(result).is_err() {
                        break;
                    }
                }
            }
        }
    }

    fn spawn_slots(db: &DB) -> anyhow::Result<Vec<StoredBlock>> {
        let mut slots = vec![];
        for item in db.iterator_cf(Rocksdb::cf_handle::<SlotIndex>(db), IteratorMode::Start) {
            let (key, value) = item.context("failed to get next item")?;
            let value =
                SlotIndexValue::decode(&value, false).context("failed to decode slot data")?;
            slots.push(StoredBlock {
                exists: true,
                dead: value.dead,
                slot: SlotIndex::decode(&key).context("failed to decode slot key")?,
                block_time: value.block_time,
                block_height: value.block_height,
                storage_id: value.storage_id,
                offset: value.offset,
                size: value.size,
            });
        }
        Ok(slots)
    }

    pub fn read_slot_indexes(
        &self,
    ) -> anyhow::Result<BoxFuture<'static, anyhow::Result<Vec<StoredBlock>>>> {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReadRequest::Slots { tx })
            .context("failed to send ReadRequest::Slots request")?;
        Ok(Box::pin(async move {
            rx.await
                .context("failed to get ReadRequest::Slots request result")?
        }))
    }

    pub fn read_tx_index(
        &self,
        signature: Signature,
    ) -> anyhow::Result<BoxFuture<'static, anyhow::Result<Option<TransactionIndexValue>>>> {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReadRequest::Transaction { signature, tx })
            .context("failed to send ReadRequest::Transaction request")?;
        Ok(Box::pin(async move {
            rx.await
                .context("failed to get ReadRequest::Transaction request result")?
        }))
    }
}
