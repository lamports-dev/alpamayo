use {
    crate::config::ConfigStorageRocksdb,
    anyhow::Context,
    richat_shared::shutdown::Shutdown,
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, ErrorKind as RocksErrorKind,
        FifoCompactOptions, Options, WriteBatchWithTransaction, WriteOptions,
    },
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

#[derive(Debug)]
struct RocksdbInner {
    db: DB,
    write_tx: mpsc::SyncSender<WriteRequestWithCallback>,
}

#[derive(Debug, Clone)]
pub struct Rocksdb {
    inner: Arc<RocksdbInner>,
}

impl Rocksdb {
    #[allow(clippy::type_complexity)]
    pub fn open(
        config: ConfigStorageRocksdb,
    ) -> anyhow::Result<(Self, Vec<(String, Option<JoinHandle<anyhow::Result<()>>>)>)> {
        let db_options = Self::get_db_options();
        let cf_descriptors = Self::cf_descriptors();

        let db = DB::open_cf_descriptors(&db_options, &config.path, cf_descriptors)
            .with_context(|| format!("failed to open rocksdb with path: {:?}", config.path))?;

        let (write_tx, write_rx) = mpsc::sync_channel(1);

        let mut threads = vec![];
        let jh = Builder::new()
            .name("rocksdbWrt".to_owned())
            .spawn(move || Self::spawn_write(write_rx))?;
        threads.push(("rocksdbWrt".to_owned(), Some(jh)));

        let inner = Arc::new(RocksdbInner { db, write_tx });
        Ok((Self { inner }, threads))
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

        vec![ColumnFamilyDescriptor::new(
            TransactionIndex::NAME,
            Self::get_cf_options(),
        )]
    }

    fn spawn_write(write_rx: mpsc::Receiver<WriteRequestWithCallback>) -> anyhow::Result<()> {
        loop {
            match write_rx.try_recv() {
                Ok((request, tx)) => {
                    //
                    todo!()
                }
                Err(mpsc::TryRecvError::Empty) => sleep(Duration::from_micros(100)),
                Err(mpsc::TryRecvError::Disconnected) => return Ok(()),
            }
        }
    }

    pub async fn send_write(&self, request: WriteRequest) -> anyhow::Result<()> {
        let (tx, rx) = oneshot::channel();
        self.inner
            .write_tx
            .send((request, tx))
            .context("failed to send write request")?;
        rx.await.context("failed to get write request result")?
    }
}

type WriteRequestWithCallback = (WriteRequest, oneshot::Sender<anyhow::Result<()>>);

#[derive(Debug)]
pub struct WriteRequest {
    //
}
