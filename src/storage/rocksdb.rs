use {
    crate::{
        config::ConfigStorageRocksdb, source::block::BlockWithBinary, storage::files::StorageId,
    },
    anyhow::Context,
    bitflags::bitflags,
    foldhash::quality::SeedableRandomState,
    futures::future::BoxFuture,
    prost::{
        bytes::Buf,
        encoding::{decode_varint, encode_varint},
    },
    rocksdb::{ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, Options, WriteBatch},
    solana_sdk::{
        clock::{Slot, UnixTimestamp},
        signature::Signature,
    },
    std::{
        hash::BuildHasher,
        sync::{Arc, Mutex, mpsc},
        thread::{Builder, JoinHandle},
    },
    tokio::sync::oneshot,
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

    fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {
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
                let mut transactions = Vec::with_capacity(slice.len() / 8);
                for i in 0..slice.len() / 8 {
                    transactions[i] = slice[i * 8..(i + 1) * 8].try_into().expect("valid slice");
                }
                transactions
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
pub struct Rocksdb {
    write_tx: mpsc::SyncSender<WriteRequest>,
    read_tx: mpsc::SyncSender<ReadRequest>,
}

impl Rocksdb {
    #[allow(clippy::type_complexity)]
    pub fn open(
        config: ConfigStorageRocksdb,
    ) -> anyhow::Result<(Self, Vec<(String, Option<JoinHandle<anyhow::Result<()>>>)>)> {
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
                Self::spawn_write(db, write_rx);
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
                    Self::spawn_read(db, read_rx);
                    Ok(())
                }
            })?;
            threads.push((th_name, Some(jh)));
        }

        Ok((Self { write_tx, read_tx }, threads))
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

    fn spawn_write(db: Arc<DB>, write_rx: mpsc::Receiver<WriteRequest>) {
        let mut buf = vec![];

        while let Ok(WriteRequest { slot, data, tx }) = write_rx.recv() {
            let mut batch = WriteBatch::with_capacity_bytes(1024 * 1024); // 1MiB

            let block = if let Some((block, storage_id, offset)) = data {
                for tx_offset in block.txs_offset.iter() {
                    buf.clear();
                    TransactionIndexValue {
                        slot,
                        offset: tx_offset.offset,
                        size: tx_offset.size,
                    }
                    .encode(&mut buf);
                    batch.put_cf(
                        Self::cf_handle::<TransactionIndex>(&db),
                        tx_offset.hash,
                        &buf,
                    );
                }

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
                Self::cf_handle::<SlotIndex>(&db),
                SlotIndex::key(slot),
                &buf,
            );

            let result = db.write(batch);
            if tx.send(result.map_err(Into::into)).is_err() {
                break;
            }
        }
    }

    pub async fn write(&self, slot: Slot, data: WriteRequestData) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.write_tx
            .send(WriteRequest { slot, data, tx })
            .context("failed to send write_tx_index request")?;
        rx.await
            .context("failed to get write_tx_index request result")?
    }

    fn spawn_read(db: Arc<DB>, read_rx: Arc<Mutex<mpsc::Receiver<ReadRequest>>>) {
        loop {
            let lock = read_rx.lock().expect("unpanicked mutex");
            let Ok(request) = lock.recv() else {
                break;
            };
            drop(lock);

            match request {
                ReadRequest::Transaction { signature, tx } => {
                    let result = match db.get_pinned_cf(
                        Self::cf_handle::<TransactionIndex>(&db),
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

    pub fn read_tx_index(
        &self,
        signature: Signature,
    ) -> anyhow::Result<BoxFuture<'static, anyhow::Result<Option<TransactionIndexValue>>>> {
        let (tx, rx) = oneshot::channel();
        self.read_tx
            .send(ReadRequest::Transaction { signature, tx })
            .context("failed to send read_tx_index request")?;
        Ok(Box::pin(async move {
            rx.await
                .context("failed to get read_tx_index request result")?
        }))
    }
}

type WriteRequestData = Option<(Arc<BlockWithBinary>, StorageId, u64)>;

#[derive(Debug)]
struct WriteRequest {
    slot: Slot,
    data: WriteRequestData,
    tx: oneshot::Sender<anyhow::Result<()>>,
}

#[derive(Debug)]
enum ReadRequest {
    Transaction {
        signature: Signature,
        tx: oneshot::Sender<anyhow::Result<Option<TransactionIndexValue>>>,
    },
}
