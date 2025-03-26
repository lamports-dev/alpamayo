use {
    crate::{config::ConfigStorageRocksdb, storage::files::StorageId},
    anyhow::Context,
    prost::encoding::encode_varint,
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, Options, WriteBatch,
        WriteOptions,
    },
    solana_sdk::clock::Slot,
    std::{
        sync::{Arc, mpsc},
        thread::{Builder, JoinHandle, sleep},
        time::Duration,
    },
    tokio::sync::oneshot,
};

mod columns {
    #[derive(Debug)]
    pub struct TransactionIndex;
}

trait ColumnName {
    const NAME: &'static str;
}

impl ColumnName for columns::TransactionIndex {
    const NAME: &'static str = "tx_index";
}

#[derive(Debug, Clone)]
pub struct Rocksdb {
    write_tx: mpsc::SyncSender<WriteRequestWithCallback>,
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

        let mut threads = vec![];
        let jh = Builder::new().name("rocksdbWrt".to_owned()).spawn({
            let db = Arc::clone(&db);
            move || Self::spawn_write(db, write_rx)
        })?;
        threads.push(("rocksdbWrt".to_owned(), Some(jh)));

        Ok((Self { write_tx }, threads))
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
        use columns::*;

        vec![Self::cf_descriptor::<TransactionIndex>()]
    }

    fn cf_descriptor<C: ColumnName>() -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options())
    }

    fn cf_handle<C: ColumnName>(db: &DB) -> &ColumnFamily {
        db.cf_handle(C::NAME)
            .expect("should never get an unknown column")
    }

    fn spawn_write(
        db: Arc<DB>,
        write_rx: mpsc::Receiver<WriteRequestWithCallback>,
    ) -> anyhow::Result<()> {
        use columns::*;

        loop {
            match write_rx.try_recv() {
                Ok((
                    WriteRequest {
                        storage_id,
                        slot,
                        block_offset,
                        txs_offset,
                    },
                    tx,
                )) => {
                    let mut batch = WriteBatch::with_capacity_bytes(256 * 1024);
                    let mut buf = Vec::with_capacity(4 * 9);
                    for (hash, offset, size) in txs_offset {
                        buf.clear();
                        encode_varint(storage_id as u64, &mut buf);
                        encode_varint(slot, &mut buf);
                        encode_varint(block_offset + offset, &mut buf);
                        encode_varint(size, &mut buf);

                        batch.put_cf(
                            Self::cf_handle::<TransactionIndex>(&db),
                            hash.to_be_bytes(),
                            &buf,
                        );
                    }

                    let options = WriteOptions::new();
                    let result = db.write_opt(batch, &options);
                    if tx.send(result.map_err(Into::into)).is_err() {
                        return Ok(());
                    }
                }
                Err(mpsc::TryRecvError::Empty) => sleep(Duration::from_micros(100)),
                Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
            }
        }
    }

    pub async fn send_write(&self, request: WriteRequest) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.write_tx
            .send((request, tx))
            .context("failed to send write request")?;
        rx.await.context("failed to get write request result")?
    }
}

type WriteRequestWithCallback = (WriteRequest, oneshot::Sender<anyhow::Result<()>>);

#[derive(Debug)]
pub struct WriteRequest {
    storage_id: StorageId,
    slot: Slot,
    block_offset: u64,
    txs_offset: Vec<(u64, u64, u64)>, // hash, offset, size
}

impl WriteRequest {
    pub fn new(
        storage_id: StorageId,
        slot: Slot,
        block_offset: u64,
        txs_offset: Vec<(u64, u64, u64)>,
    ) -> Self {
        Self {
            storage_id,
            slot,
            block_offset,
            txs_offset,
        }
    }
}
