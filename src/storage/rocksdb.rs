use {
    crate::{
        config::ConfigStorageRocksdb,
        source::{block::BlockWithBinary, sfa::SignatureStatus},
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
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, Direction, IteratorMode,
        Options, WriteBatch,
    },
    solana_rpc_client_api::response::RpcConfirmedTransactionStatusWithSignature,
    solana_sdk::{
        clock::{Slot, UnixTimestamp},
        pubkey::Pubkey,
        signature::Signature,
        transaction::TransactionError,
    },
    std::{
        borrow::Cow,
        hash::BuildHasher,
        sync::{Arc, Mutex, mpsc},
        thread::{Builder, JoinHandle},
    },
    tokio::sync::{broadcast, oneshot},
};

thread_local! {
    static HASHER: SeedableRandomState = SeedableRandomState::fixed();
}

trait ColumnName {
    const NAME: &'static str;
}

#[derive(Debug)]
pub struct SlotBasicIndex;

impl ColumnName for SlotBasicIndex {
    const NAME: &'static str = "slot_basic_index";
}

impl SlotBasicIndex {
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
pub struct SlotBasicIndexValue {
    pub dead: bool,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<Slot>,
    pub storage_id: StorageId,
    pub offset: u64,
    pub size: u64,
}

impl SlotBasicIndexValue {
    fn encode(&self, buf: &mut Vec<u8>) {
        if self.dead {
            encode_varint(SlotBasicIndexValueFlags::DEAD.bits() as u64, buf);
            return;
        }

        let mut flags = SlotBasicIndexValueFlags::empty();
        if self.dead {
            flags |= SlotBasicIndexValueFlags::DEAD;
        }
        if self.block_time.is_some() {
            flags |= SlotBasicIndexValueFlags::BLOCK_TIME;
        }
        if self.block_height.is_some() {
            flags |= SlotBasicIndexValueFlags::BLOCK_HEIGHT;
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
    }

    fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {
        let flags = SlotBasicIndexValueFlags::from_bits(
            slice.try_get_u8().context("failed to read flags")?,
        )
        .context("invalid flags")?;

        if flags.contains(SlotBasicIndexValueFlags::DEAD) {
            return Ok(Self {
                dead: true,
                ..Default::default()
            });
        }

        Ok(Self {
            dead: false,
            block_time: flags
                .contains(SlotBasicIndexValueFlags::BLOCK_TIME)
                .then(|| decode_varint(&mut slice).map(|bt| bt as i64))
                .transpose()
                .context("failed to read block time")?,
            block_height: flags
                .contains(SlotBasicIndexValueFlags::BLOCK_HEIGHT)
                .then(|| decode_varint(&mut slice))
                .transpose()
                .context("failed to read block_height")?,
            storage_id: decode_varint(&mut slice)
                .context("failed to read storage id")?
                .try_into()
                .context("failed to convert storage id")?,
            offset: decode_varint(&mut slice).context("failed to read offset")?,
            size: decode_varint(&mut slice).context("failed to read size")?,
        })
    }
}

bitflags! {
    struct SlotBasicIndexValueFlags: u8 {
        const DEAD =         0b00000001;
        const BLOCK_TIME =   0b00000010;
        const BLOCK_HEIGHT = 0b00000100;
    }
}

#[derive(Debug)]
pub struct SlotExtraIndex;

impl ColumnName for SlotExtraIndex {
    const NAME: &'static str = "slot_extra_index";
}

impl SlotExtraIndex {
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
pub struct SlotExtraIndexValue {
    pub transactions: Vec<[u8; 8]>,
    pub sfa: Vec<[u8; 8]>,
}

impl SlotExtraIndexValue {
    fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint(self.transactions.len() as u64, buf);
        for hash in self.transactions.iter() {
            buf.extend_from_slice(hash);
        }
        encode_varint(self.sfa.len() as u64, buf);
        for hash in self.sfa.iter() {
            buf.extend_from_slice(hash);
        }
    }

    fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {
        let mut transactions = Vec::with_capacity(
            decode_varint(&mut slice)
                .context("failed to read transactions size")?
                .try_into()
                .context("failed to convert transactions size")?,
        );
        for i in 0..transactions.capacity() {
            let hash = slice[i * 8..(i + 1) * 8].try_into().expect("valid slice");
            transactions.push(hash);
        }
        slice.advance(transactions.len() * 8);

        let mut sfa = Vec::with_capacity(
            decode_varint(&mut slice)
                .context("failed to read sfa size")?
                .try_into()
                .context("failed to convert sfa size")?,
        );
        for i in 0..sfa.capacity() {
            let hash = slice[i * 8..(i + 1) * 8].try_into().expect("valid slice");
            sfa.push(hash);
        }
        slice.advance(sfa.len() * 8);
        anyhow::ensure!(slice.is_empty(), "invalid slice len, left: {}", slice.len());

        Ok(Self { transactions, sfa })
    }
}

#[derive(Debug)]
pub struct TransactionIndex;

impl ColumnName for TransactionIndex {
    const NAME: &'static str = "tx_index";
}

impl TransactionIndex {
    pub fn encode(signature: &Signature) -> [u8; 8] {
        let hash = HASHER.with(|hasher| hasher.hash_one(signature));
        hash.to_be_bytes()
    }
}

#[derive(Debug, Clone)]
pub struct TransactionIndexValue<'a> {
    pub slot: Slot,
    pub offset: u64,
    pub size: u64,
    pub err: Option<Cow<'a, TransactionError>>,
}

impl TransactionIndexValue<'_> {
    fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint(self.slot, buf);
        encode_varint(self.offset, buf);
        encode_varint(self.size, buf);
        if let Some(err) = &self.err {
            let data = bincode::serialize(err).expect("bincode never fail");
            encode_varint(data.len() as u64, buf);
            buf.extend_from_slice(&data);
        }
    }

    fn decode(mut slice: &[u8], decode_error: bool) -> anyhow::Result<Self> {
        Ok(Self {
            slot: decode_varint(&mut slice).context("failed to decode slot")?,
            offset: decode_varint(&mut slice).context("failed to decode offset")?,
            size: decode_varint(&mut slice).context("failed to decode size")?,
            err: if slice.is_empty() || !decode_error {
                None
            } else {
                let size = decode_varint(&mut slice).context("failed to decode err size")? as usize;
                anyhow::ensure!(
                    slice.remaining() == size,
                    "invalid slice len to decode err, expected {} left {}",
                    size,
                    slice.remaining()
                );
                let err = bincode::deserialize(&slice[0..size]).context("failed to decode err")?;
                Some(Cow::Owned(err))
            },
        })
    }
}

#[derive(Debug)]
pub struct SfaIndex;

impl ColumnName for SfaIndex {
    const NAME: &'static str = "sfa_index";
}

impl SfaIndex {
    pub fn address_hash(address: &Pubkey) -> [u8; 8] {
        HASHER.with(|hasher| hasher.hash_one(address)).to_be_bytes()
    }

    pub fn concat(address_hash: [u8; 8], slot: Slot) -> [u8; 16] {
        let mut key = [0; 16];
        key[0..8].copy_from_slice(&address_hash);
        key[8..].copy_from_slice(&slot.to_be_bytes());
        key
    }

    pub fn encode(address: &Pubkey, slot: Slot) -> [u8; 16] {
        Self::concat(Self::address_hash(address), slot)
    }

    pub fn decode(slice: &[u8]) -> anyhow::Result<([u8; 8], Slot)> {
        anyhow::ensure!(slice.len() == 16, "invalid key length: {}", slice.len());
        Ok((
            slice[0..8].try_into().expect("valid len"),
            Slot::from_be_bytes(slice[8..].try_into().expect("valid len")),
        ))
    }
}

#[derive(Debug)]
pub struct SfaIndexValue<'a> {
    signatures: Cow<'a, [SignatureStatus]>,
}

impl SfaIndexValue<'_> {
    fn encode(&self, buf: &mut Vec<u8>) {
        for sig in self.signatures.iter() {
            let mut fields = SfaIndexValueFlags::empty();
            if sig.err.is_some() {
                fields |= SfaIndexValueFlags::ERR;
            }
            if sig.memo.is_some() {
                fields |= SfaIndexValueFlags::MEMO;
            }
            buf.push(fields.bits());

            buf.extend_from_slice(sig.signature.as_ref());
            if let Some(err) = &sig.err {
                let data = bincode::serialize(err).expect("bincode never fail");
                encode_varint(data.len() as u64, buf);
                buf.extend_from_slice(&data);
            }
            if let Some(memo) = &sig.memo {
                encode_varint(memo.len() as u64, buf);
                buf.extend_from_slice(memo.as_ref());
            }
        }
    }

    fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {
        let mut sigs = vec![];
        while !slice.is_empty() {
            let flags =
                SfaIndexValueFlags::from_bits(slice.try_get_u8().context("failed to read flags")?)
                    .context("invalid flags")?;

            let mut signature = [0u8; 64];
            slice
                .try_copy_to_slice(&mut signature)
                .context("failed to read signature")?;

            let err = if flags.contains(SfaIndexValueFlags::ERR) {
                let size = decode_varint(&mut slice).context("failed to decode err size")? as usize;
                anyhow::ensure!(slice.remaining() >= size, "not enough bytes for memo");
                let err = bincode::deserialize(&slice[0..size]).context("failed to decode err")?;
                slice.advance(size);
                Some(err)
            } else {
                None
            };

            let memo = if flags.contains(SfaIndexValueFlags::MEMO) {
                let size =
                    decode_varint(&mut slice).context("failed to decode memo size")? as usize;
                anyhow::ensure!(slice.remaining() >= size, "not enough bytes for memo");
                let memo =
                    String::from_utf8(slice[0..size].to_vec()).context("expect utf8 memo")?;
                slice.advance(size);
                Some(memo)
            } else {
                None
            };

            sigs.push(SignatureStatus {
                signature: signature.into(),
                err,
                memo,
            });
        }
        Ok(Self {
            signatures: Cow::Owned(sigs),
        })
    }
}

bitflags! {
    #[derive(Debug)]
    struct SfaIndexValueFlags: u8 {
        const ERR =  0b00000001;
        const MEMO = 0b00000010;
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
        let cf_descriptors = Self::cf_descriptors(
            config.index_slot_compression.into(),
            config.index_sfa_compression.into(),
        );

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

    fn get_cf_options(compression: DBCompressionType) -> Options {
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

        options.set_compression_type(compression);

        options
    }

    fn cf_descriptors(
        index_slot_compression: DBCompressionType,
        index_sfa_compression: DBCompressionType,
    ) -> Vec<ColumnFamilyDescriptor> {
        vec![
            Self::cf_descriptor::<SlotBasicIndex>(DBCompressionType::None),
            Self::cf_descriptor::<SlotExtraIndex>(index_slot_compression),
            Self::cf_descriptor::<TransactionIndex>(DBCompressionType::None),
            Self::cf_descriptor::<SfaIndex>(index_sfa_compression),
        ]
    }

    fn cf_descriptor<C: ColumnName>(compression: DBCompressionType) -> ColumnFamilyDescriptor {
        ColumnFamilyDescriptor::new(C::NAME, Self::get_cf_options(compression))
    }

    fn cf_handle<C: ColumnName>(db: &DB) -> &ColumnFamily {
        db.cf_handle(C::NAME)
            .expect("should never get an unknown column")
    }
}

#[derive(Debug)]
enum WriteRequest {
    AddIndexes {
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
                WriteRequest::AddIndexes { slot, block, tx } => {
                    let mut batch = WriteBatch::with_capacity_bytes(2 * 1024 * 1024); // 2MiB
                    buf.clear();
                    SlotExtraIndexValue {
                        transactions: block.txs_offset.iter().map(|txo| txo.key).collect(),
                        sfa: block.sfa.values().map(|sfa| sfa.address_hash).collect(),
                    }
                    .encode(&mut buf);
                    batch.put_cf(
                        Rocksdb::cf_handle::<SlotExtraIndex>(&db),
                        SlotExtraIndex::key(slot),
                        &buf,
                    );
                    for tx_offset in block.txs_offset.iter() {
                        buf.clear();
                        TransactionIndexValue {
                            slot,
                            offset: tx_offset.offset,
                            size: tx_offset.size,
                            err: tx_offset.err.as_ref().map(Cow::Borrowed),
                        }
                        .encode(&mut buf);
                        batch.put_cf(
                            Rocksdb::cf_handle::<TransactionIndex>(&db),
                            tx_offset.key,
                            &buf,
                        );
                    }
                    for sfa in block.sfa.values() {
                        buf.clear();
                        SfaIndexValue {
                            signatures: Cow::Borrowed(sfa.signatures.as_slice()),
                        }
                        .encode(&mut buf);
                        batch.put_cf(Rocksdb::cf_handle::<SfaIndex>(&db), sfa.key, &buf);
                    }
                    if tx.send(db.write(batch).map_err(Into::into)).is_err() {
                        break;
                    }
                }
                WriteRequest::SlotAdd { slot, data, tx } => {
                    buf.clear();
                    if let Some((block, storage_id, offset)) = data {
                        SlotBasicIndexValue {
                            dead: false,
                            block_time: block.block_time,
                            block_height: block.block_height,
                            storage_id,
                            offset,
                            size: block.protobuf.len() as u64,
                        }
                    } else {
                        SlotBasicIndexValue {
                            dead: true,
                            ..Default::default()
                        }
                    }
                    .encode(&mut buf);

                    let result = db.put_cf(
                        Rocksdb::cf_handle::<SlotBasicIndex>(&db),
                        SlotBasicIndex::key(slot),
                        &buf,
                    );
                    if tx.send(result.map_err(Into::into)).is_err() {
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
            .get_pinned_cf(
                Rocksdb::cf_handle::<SlotExtraIndex>(db),
                SlotExtraIndex::key(slot),
            )
            .context("failed to get existed slot")?
            .ok_or_else(|| anyhow::anyhow!("existed slot {slot} not found"))?;
        let block = SlotExtraIndexValue::decode(&value).context("failed to decode slot data")?;

        let mut batch = WriteBatch::with_capacity_bytes(128 * 1024); // 128KiB
        batch.delete_cf(
            Rocksdb::cf_handle::<SlotBasicIndex>(db),
            SlotBasicIndex::key(slot),
        );
        batch.delete_cf(
            Rocksdb::cf_handle::<SlotExtraIndex>(db),
            SlotExtraIndex::key(slot),
        );
        for hash in block.transactions {
            batch.delete_cf(Rocksdb::cf_handle::<TransactionIndex>(db), hash);
        }
        for hash in block.sfa {
            batch.delete_cf(
                Rocksdb::cf_handle::<SfaIndex>(db),
                SfaIndex::concat(hash, slot),
            );
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
        // 2) tx and sfa index in db
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
                    .send(WriteRequest::AddIndexes {
                        slot,
                        block: Arc::clone(&block),
                        tx,
                    })
                    .context("failed to send WriteRequest::TxSfaIndex request")?;
                rx.await
                    .context("failed to get WriteRequest::TxSfaIndex request result")??;
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
        decode_error: bool,
        tx: oneshot::Sender<anyhow::Result<Option<TransactionIndexValue<'static>>>>,
    },
    SignaturesForAddress {
        address: Pubkey,
        slot: Slot,
        before: Option<Signature>,
        until: Signature,
        signatures: Vec<RpcConfirmedTransactionStatusWithSignature>,
        tx: oneshot::Sender<
            anyhow::Result<(Vec<RpcConfirmedTransactionStatusWithSignature>, bool)>,
        >,
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
                ReadRequest::Transaction {
                    signature,
                    decode_error,
                    tx,
                } => {
                    let result = match db.get_pinned_cf(
                        Rocksdb::cf_handle::<TransactionIndex>(&db),
                        TransactionIndex::encode(&signature),
                    ) {
                        Ok(Some(slice)) => {
                            TransactionIndexValue::decode(slice.as_ref(), decode_error).map(Some)
                        }
                        Ok(None) => Ok(None),
                        Err(error) => Err(anyhow::anyhow!("failed to get tx location: {error:?}")),
                    };

                    if tx.send(result).is_err() {
                        break;
                    }
                }
                ReadRequest::SignaturesForAddress {
                    address,
                    slot,
                    before,
                    until,
                    signatures,
                    tx,
                } => {
                    if tx
                        .send(Self::spawn_signatires_for_address(
                            &db, address, slot, before, until, signatures,
                        ))
                        .is_err()
                    {
                        break;
                    }
                }
            }
        }
    }

    fn spawn_slots(db: &DB) -> anyhow::Result<Vec<StoredBlock>> {
        let mut slots = vec![];
        for item in db.iterator_cf(
            Rocksdb::cf_handle::<SlotBasicIndex>(db),
            IteratorMode::Start,
        ) {
            let (key, value) = item.context("failed to get next item")?;
            let value =
                SlotBasicIndexValue::decode(&value).context("failed to decode slot data")?;
            slots.push(StoredBlock {
                exists: true,
                dead: value.dead,
                slot: SlotBasicIndex::decode(&key).context("failed to decode slot key")?,
                block_time: value.block_time,
                block_height: value.block_height,
                storage_id: value.storage_id,
                offset: value.offset,
                size: value.size,
            });
        }
        Ok(slots)
    }

    fn spawn_signatires_for_address(
        db: &DB,
        address: Pubkey,
        slot: Slot,
        mut before: Option<Signature>,
        until: Signature,
        mut signatures: Vec<RpcConfirmedTransactionStatusWithSignature>,
    ) -> anyhow::Result<(Vec<RpcConfirmedTransactionStatusWithSignature>, bool)> {
        let address_hash = SfaIndex::address_hash(&address);
        let key = SfaIndex::concat(address_hash, slot);
        let mut finished = false;
        'outer: for item in db.iterator_cf(
            Rocksdb::cf_handle::<SfaIndex>(db),
            IteratorMode::From(&key, Direction::Reverse),
        ) {
            let (key, value) = item.context("failed to read next row")?;
            let (item_address_hash, slot) = SfaIndex::decode(&key)?;
            if item_address_hash != address_hash {
                break;
            }

            #[allow(clippy::unnecessary_to_owned)] // looks like clippy bug
            for sigstatus in SfaIndexValue::decode(&value)?.signatures.into_owned() {
                if let Some(sigbefore) = before {
                    if sigstatus.signature == sigbefore {
                        before = None;
                    }
                    continue;
                }

                if sigstatus.signature == until {
                    finished = true;
                    break 'outer;
                }

                signatures.push(RpcConfirmedTransactionStatusWithSignature {
                    signature: sigstatus.signature.to_string(),
                    slot,
                    err: sigstatus.err,
                    memo: sigstatus.memo,
                    block_time: None,
                    confirmation_status: None,
                });

                if signatures.len() == signatures.capacity() {
                    finished = true;
                    break 'outer;
                }
            }
        }
        Ok((signatures, finished))
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
        decode_error: bool,
    ) -> anyhow::Result<BoxFuture<'static, anyhow::Result<Option<TransactionIndexValue<'static>>>>>
    {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReadRequest::Transaction {
                signature,
                decode_error,
                tx,
            })
            .context("failed to send ReadRequest::Transaction request")?;
        Ok(Box::pin(async move {
            rx.await
                .context("failed to get ReadRequest::Transaction request result")?
        }))
    }

    #[allow(clippy::type_complexity)]
    pub fn read_signatures_for_address(
        &self,
        address: Pubkey,
        slot: Slot,
        before: Option<Signature>,
        until: Signature,
        signatures: Vec<RpcConfirmedTransactionStatusWithSignature>,
    ) -> anyhow::Result<
        BoxFuture<'static, anyhow::Result<(Vec<RpcConfirmedTransactionStatusWithSignature>, bool)>>,
    > {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReadRequest::SignaturesForAddress {
                address,
                slot,
                before,
                until,
                signatures,
                tx,
            })
            .context("failed to send ReadRequest::SignaturesForAddress request")?;
        Ok(Box::pin(async move {
            rx.await
                .context("failed to get ReadRequest::SignaturesForAddress request result")?
        }))
    }
}
