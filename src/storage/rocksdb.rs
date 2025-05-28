use {
    crate::{
        config::ConfigStorageRocksdb,
        source::{block::BlockWithBinary, sfa::SignatureStatus},
        storage::{
            blocks::{StoredBlock, StoredBlocksWrite},
            files::{StorageFilesWrite, StorageId},
            sync::ReadWriteSyncMessage,
        },
        util::{HashMap, VecSide},
    },
    anyhow::Context,
    bitflags::bitflags,
    bitvec::vec::BitVec,
    foldhash::quality::SeedableRandomState,
    futures::future::BoxFuture,
    prost::{
        bytes::{Buf, BufMut},
        encoding::{decode_varint, encode_varint},
    },
    quanta::Instant,
    rocksdb::{
        ColumnFamily, ColumnFamilyDescriptor, DB, DBCompressionType, Direction, IteratorMode,
        Options, WriteBatch,
    },
    solana_rpc_client_api::response::{
        RpcConfirmedTransactionStatusWithSignature, RpcInflationReward,
    },
    solana_sdk::{
        clock::{Epoch, Slot, UnixTimestamp},
        epoch_rewards_hasher::EpochRewardsHasher,
        hash::{HASH_BYTES, Hash},
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
    tracing::{error, info},
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
    pub const fn key(slot: Slot) -> [u8; 8] {
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
    pub const fn key(slot: Slot) -> [u8; 8] {
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

#[derive(Debug)]
pub struct InflationRewardIndex;

impl ColumnName for InflationRewardIndex {
    const NAME: &'static str = "ir_index";
}

impl InflationRewardIndex {
    const fn encode_base(epoch: Epoch) -> [u8; 8] {
        epoch.to_be_bytes()
    }

    fn encode_reward(epoch: Epoch, pubkey: Pubkey) -> [u8; 40] {
        let mut key = [0u8; 40];
        key[0..8].copy_from_slice(&epoch.to_be_bytes());
        key[8..].copy_from_slice(pubkey.as_ref());
        key
    }
}

#[derive(Debug)]
pub struct InflationRewardBaseValue {
    pub slot: Slot,
    pub block_height: Option<Slot>,
    pub previous_blockhash: Hash,
    pub num_reward_partitions: Option<u64>,
    partitions: BitVec<u8>,
}

impl InflationRewardBaseValue {
    pub fn new(
        slot: Slot,
        block_height: Option<Slot>,
        previous_blockhash: Hash,
        num_reward_partitions: Option<u64>,
    ) -> Self {
        Self {
            slot,
            block_height,
            previous_blockhash,
            num_reward_partitions,
            partitions: BitVec::repeat(false, num_reward_partitions.unwrap_or(0) as usize),
        }
    }

    fn encode(self, buf: &mut Vec<u8>) {
        let mut flags = 0u8;
        if self.block_height.is_some() {
            flags |= 0b01;
        }
        if self.num_reward_partitions.is_some() {
            flags |= 0b10;
        }
        buf.put_u8(flags);

        encode_varint(self.slot, buf);
        if let Some(block_height) = self.block_height {
            encode_varint(block_height, buf);
        }
        buf.extend_from_slice(self.previous_blockhash.as_ref());
        if let Some(num_reward_partitions) = self.num_reward_partitions {
            encode_varint(num_reward_partitions, buf);
        }
        let vec = self.partitions.into_vec();
        encode_varint(vec.len() as u64, buf);
        buf.extend_from_slice(vec.as_ref());
    }

    fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {
        anyhow::ensure!(slice.remaining() >= 1, "not enough bytes for flags");
        let flags = slice[0];
        slice.advance(1);

        let slot = decode_varint(&mut slice).context("failed to decode slot")?;
        let block_height = if (flags & 0b01) > 0 {
            Some(decode_varint(&mut slice).context("failed to decode block height")?)
        } else {
            None
        };
        anyhow::ensure!(
            slice.remaining() >= HASH_BYTES,
            "not enough bytes for previous blockhash"
        );
        let previous_blockhash = Hash::new_from_array(slice[0..HASH_BYTES].try_into().unwrap());
        slice.advance(HASH_BYTES);
        let num_reward_partitions = if (flags & 0b10) > 0 {
            Some(decode_varint(&mut slice).context("failed to decode num reward partitions")?)
        } else {
            None
        };
        let len = decode_varint(&mut slice).context("failed to decode partitions len")? as usize;
        anyhow::ensure!(slice.remaining() >= len, "not enough bytes for partitions");
        let vec = slice[0..len].to_vec();

        Ok(Self {
            slot,
            block_height,
            previous_blockhash,
            num_reward_partitions,
            partitions: BitVec::from_vec(vec),
        })
    }
}

#[derive(Debug)]
pub struct InflationRewardAddressValue {
    reward: RpcInflationReward,
}

impl InflationRewardAddressValue {
    fn encode(&self, buf: &mut Vec<u8>) {
        encode_varint(self.reward.epoch, buf);
        encode_varint(self.reward.effective_slot, buf);
        encode_varint(self.reward.amount, buf);
        encode_varint(self.reward.post_balance, buf);
        if let Some(comission) = self.reward.commission {
            buf.put_u8(comission);
        }
    }

    fn decode(mut slice: &[u8]) -> anyhow::Result<Self> {
        let epoch = decode_varint(&mut slice).context("failed to decode epoch")?;
        let effective_slot =
            decode_varint(&mut slice).context("failed to decode effective_slot")?;
        let amount = decode_varint(&mut slice).context("failed to decode amount")?;
        let post_balance = decode_varint(&mut slice).context("failed to decode post_balance")?;
        let commission = if slice.is_empty() {
            None
        } else {
            Some(slice[0])
        };

        Ok(Self {
            reward: RpcInflationReward {
                epoch,
                effective_slot,
                amount,
                post_balance,
                commission,
            },
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
        RocksdbWriteInflationReward,
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
        let (read_tx, read_rx) = mpsc::channel();

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
                req_tx: write_tx.clone(),
                sync_tx,
            },
            RocksdbWriteInflationReward { req_tx: write_tx },
            RocksdbRead { req_tx: read_tx },
            threads,
        ))
    }

    fn get_db_options() -> Options {
        let mut options = Options::default();

        // Create if not exists
        options.create_if_missing(true);
        options.create_missing_column_families(true);

        // Set_max_background_jobs(N), configures N/4 low priority threads and 3N/4 high priority threads
        options.set_max_background_jobs(num_cpus::get() as i32);

        // Set max total WAL size to 4GiB
        options.set_max_total_wal_size(4 * 1024 * 1024 * 1024);

        options
    }

    fn get_cf_options(compression: DBCompressionType) -> Options {
        let mut options = Options::default();

        const MAX_WRITE_BUFFER_SIZE: u64 = 256 * 1024 * 1024;
        options.set_max_write_buffer_number(8);
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
            Self::cf_descriptor::<InflationRewardIndex>(DBCompressionType::None),
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
        dead: bool,
        tx: oneshot::Sender<anyhow::Result<()>>,
    },
    InflationRewardBase {
        epoch: Epoch,
        slot: Slot,
        block_height: Option<Slot>,
        previous_blockhash: Hash,
        num_reward_partitions: Option<u64>,
        reward_map: HashMap<Pubkey, RpcInflationReward>,
    },
    InflationRewardPartition {
        epoch: Epoch,
        partition_index: usize,
        reward_map: HashMap<Pubkey, RpcInflationReward>,
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
                WriteRequest::SlotRemove { slot, dead, tx } => {
                    if tx.send(Self::spawn_remove_slot(&db, slot, dead)).is_err() {
                        break;
                    }
                }
                WriteRequest::InflationRewardBase {
                    epoch,
                    slot,
                    block_height,
                    previous_blockhash,
                    num_reward_partitions,
                    reward_map,
                } => {
                    let ts = Instant::now();
                    if let Err(error) = Self::spawn_push_reward(
                        &db,
                        epoch,
                        InflationRewardBaseValue::new(
                            slot,
                            block_height,
                            previous_blockhash,
                            num_reward_partitions,
                        ),
                        reward_map,
                        &mut buf,
                    ) {
                        error!(?error, epoch, elapsed = ?ts.elapsed(), "failed to savebase inflation reward");
                    } else {
                        info!(epoch, elapsed = ?ts.elapsed(), "save base inflation reward");
                    }
                }
                WriteRequest::InflationRewardPartition {
                    epoch,
                    partition_index,
                    reward_map,
                } => {
                    let ts = Instant::now();
                    if let Err(error) = Self::spawn_push_partition_reward(
                        &db,
                        epoch,
                        partition_index,
                        reward_map,
                        &mut buf,
                    ) {
                        error!(?error, epoch, partition_index, elapsed = ?ts.elapsed(), "failed to save partition inflation reward");
                    } else {
                        info!(epoch, partition_index, elapsed = ?ts.elapsed(), "save partition inflation reward");
                    }
                }
            }
        }
    }

    fn spawn_remove_slot(db: &DB, slot: Slot, dead: bool) -> anyhow::Result<()> {
        let mut batch = WriteBatch::with_capacity_bytes(128 * 1024); // 128KiB
        batch.delete_cf(
            Rocksdb::cf_handle::<SlotBasicIndex>(db),
            SlotBasicIndex::key(slot),
        );

        if !dead {
            let value = db
                .get_pinned_cf(
                    Rocksdb::cf_handle::<SlotExtraIndex>(db),
                    SlotExtraIndex::key(slot),
                )
                .context("failed to get existed slot")?
                .ok_or_else(|| anyhow::anyhow!("existed slot {slot} not found"))?;
            let block =
                SlotExtraIndexValue::decode(&value).context("failed to decode slot data")?;
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
        }

        db.write(batch).map_err(Into::into)
    }

    fn spawn_push_partition_reward(
        db: &DB,
        epoch: Epoch,
        partition_index: usize,
        reward_map: HashMap<Pubkey, RpcInflationReward>,
        buf: &mut Vec<u8>,
    ) -> anyhow::Result<()> {
        let value = db
            .get_pinned_cf(
                Rocksdb::cf_handle::<InflationRewardIndex>(db),
                InflationRewardIndex::encode_base(epoch),
            )
            .context("failed to get existed epoch reward")?
            .ok_or_else(|| anyhow::anyhow!("existed epoch {epoch} not found"))?;
        let mut base = InflationRewardBaseValue::decode(&value)?;
        anyhow::ensure!(
            base.partitions.len() >= partition_index,
            "not enough partitions"
        );
        base.partitions.set(partition_index, true);

        Self::spawn_push_reward(db, epoch, base, reward_map, buf).map_err(Into::into)
    }

    fn spawn_push_reward(
        db: &DB,
        epoch: Epoch,
        base: InflationRewardBaseValue,
        reward_map: HashMap<Pubkey, RpcInflationReward>,
        buf: &mut Vec<u8>,
    ) -> Result<(), rocksdb::Error> {
        let mut batch = WriteBatch::with_capacity_bytes(2 * 1024 * 1024); // 2MiB

        buf.clear();
        base.encode(buf);
        batch.put_cf(
            Rocksdb::cf_handle::<InflationRewardIndex>(db),
            InflationRewardIndex::encode_base(epoch),
            &buf,
        );

        for (pubkey, reward) in reward_map {
            buf.clear();
            InflationRewardAddressValue { reward }.encode(buf);
            batch.put_cf(
                Rocksdb::cf_handle::<InflationRewardIndex>(db),
                InflationRewardIndex::encode_reward(epoch, pubkey),
                &buf,
            );
        }

        db.write(batch)
    }

    pub async fn push_block_back(
        &self,
        slot: Slot,
        block: Option<Arc<BlockWithBinary>>,
        files: &mut StorageFilesWrite,
        blocks: &mut StoredBlocksWrite,
    ) -> anyhow::Result<bool> {
        if let Some(next_slot) = blocks.get_back_slot().map(|slot| slot - 1) {
            anyhow::ensure!(
                next_slot == slot,
                "trying to push back invalid slot: {slot}, expected {next_slot}"
            );
        }
        if let Some((back_slot, back_height)) = blocks.get_back_height() {
            if let Some(block) = &block {
                let block_height = block.block_height.expect("should have height");
                anyhow::ensure!(
                    block_height + 1 == back_height,
                    "trying to push back block with invalid height: {block_height} (slot {slot}), current: {back_height} (slot {back_slot})",
                );
            }
        }

        // make sure that we not reached blocks limit
        if blocks.is_full() {
            return Ok(false);
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
            blocks.push_block_back_dead(slot)?;

            return Ok(true);
        };

        // 1) store block in file
        // 2) tx and sfa index in db
        let ((buffer, files_result), block) =
            tokio::try_join!(files.push_block_back(block.protobuf.clone()), async move {
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
            })?;
        let Some((storage_id, offset)) = files_result else {
            return Ok(false);
        };

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

        blocks.push_block_back_confirmed(
            slot,
            block_time,
            block_height,
            storage_id,
            offset,
            buffer.len() as u64,
        )?;
        Ok(true)
    }

    pub async fn push_block_front(
        &self,
        slot: Slot,
        block: Option<Arc<BlockWithBinary>>,
        files: &mut StorageFilesWrite,
        blocks: &mut StoredBlocksWrite,
    ) -> anyhow::Result<()> {
        if let Some(next_slot) = blocks.get_front_slot().map(|slot| slot + 1) {
            anyhow::ensure!(
                next_slot == slot,
                "trying to push front invalid slot: {slot}, expected {next_slot}"
            );
        }
        if let Some((front_slot, front_height)) = blocks.get_front_height() {
            if let Some(block) = &block {
                let block_height = block.block_height.expect("should have height");
                anyhow::ensure!(
                    front_height + 1 == block_height,
                    "trying to push front block with invalid height: {block_height} (slot {slot}), current: {front_height} (slot {front_slot})"
                );
            }
        }

        // get some space if we reached blocks limit
        if blocks.is_full() {
            self.pop_block_back(files, blocks).await?;
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
            blocks.push_block_front_dead(slot)?;

            return Ok(());
        };

        // 1) store block in file
        // 2) tx and sfa index in db
        let mut buffer = block.protobuf.clone();
        let ((storage_id, offset, buffer, blocks), block) = tokio::try_join!(
            async move {
                loop {
                    let (buffer2, result) = files.push_block_front(buffer).await?;
                    buffer = buffer2;

                    if let Some((storage_id, offset)) = result {
                        return Ok((storage_id, offset, buffer, blocks));
                    }

                    self.pop_block_back(files, blocks).await?;
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

        blocks.push_block_front_confirmed(
            slot,
            block_time,
            block_height,
            storage_id,
            offset,
            buffer.len() as u64,
        )
    }

    pub async fn pop_block_back(
        &self,
        files: &mut StorageFilesWrite,
        blocks: &mut StoredBlocksWrite,
    ) -> anyhow::Result<()> {
        self.pop_block(files, blocks, VecSide::Back).await
    }

    pub async fn pop_block_front(
        &self,
        files: &mut StorageFilesWrite,
        blocks: &mut StoredBlocksWrite,
    ) -> anyhow::Result<()> {
        self.pop_block(files, blocks, VecSide::Front).await
    }

    async fn pop_block(
        &self,
        files: &mut StorageFilesWrite,
        blocks: &mut StoredBlocksWrite,
        side: VecSide,
    ) -> anyhow::Result<()> {
        let Some(block) = (match side {
            VecSide::Back => blocks.pop_block_back(),
            VecSide::Front => blocks.pop_block_front(),
        }) else {
            anyhow::bail!("no blocks to remove from front");
        };
        let _ = self.sync_tx.send(match side {
            VecSide::Back => ReadWriteSyncMessage::ConfirmedBlockPopBack,
            VecSide::Front => ReadWriteSyncMessage::ConfirmedBlockPopFront,
        });

        // remove from db
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(WriteRequest::SlotRemove {
                slot: block.slot,
                dead: block.dead,
                tx,
            })
            .context("failed to send WriteRequest::SlotRemove request")?;
        rx.await
            .context("failed to get WriteRequest::SlotRemove request result")??;

        // update offset in file
        if block.size == 0 {
            Ok(())
        } else {
            match side {
                VecSide::Back => files.pop_block_back(block),
                VecSide::Front => files.pop_block_front(block),
            }
        }
    }
}

#[derive(Debug)]
pub struct RocksdbWriteInflationReward {
    req_tx: mpsc::SyncSender<WriteRequest>,
}

impl RocksdbWriteInflationReward {
    pub fn push_base(
        &self,
        epoch: Epoch,
        slot: Slot,
        block_height: Option<Slot>,
        previous_blockhash: Hash,
        num_reward_partitions: Option<u64>,
        reward_map: HashMap<Pubkey, RpcInflationReward>,
    ) {
        if let Err(error) = self.req_tx.send(WriteRequest::InflationRewardBase {
            epoch,
            slot,
            block_height,
            previous_blockhash,
            num_reward_partitions,
            reward_map,
        }) {
            error!(
                ?error,
                epoch, epoch, "failed to send inflation reward base write request"
            );
        }
    }

    pub fn push_partition(
        &self,
        epoch: Epoch,
        partition_index: usize,
        reward_map: HashMap<Pubkey, RpcInflationReward>,
    ) {
        if let Err(error) = self.req_tx.send(WriteRequest::InflationRewardPartition {
            epoch,
            partition_index,
            reward_map,
        }) {
            error!(
                ?error,
                epoch, epoch, "failed to send inflation reward partition write request"
            );
        }
    }
}

#[derive(Debug)]
pub struct ReadRequestResultInflationReward {
    pub addresses: Vec<Pubkey>,
    pub rewards: Vec<Option<RpcInflationReward>>,
    pub missed: Vec<usize>,
    pub base: Option<InflationRewardBaseValue>,
}

#[derive(Debug)]
enum ReadRequest {
    Slots {
        tx: oneshot::Sender<anyhow::Result<Vec<StoredBlock>>>,
    },
    Transaction {
        signature: Signature,
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
    SignatureStatuses {
        signatures: Vec<Signature>,
        tx: oneshot::Sender<anyhow::Result<Vec<(Signature, TransactionIndexValue<'static>)>>>,
    },
    InflationReward {
        epoch: Epoch,
        addresses: Vec<Pubkey>,
        tx: oneshot::Sender<anyhow::Result<ReadRequestResultInflationReward>>,
    },
}

#[derive(Debug, Clone)]
pub struct RocksdbRead {
    req_tx: mpsc::Sender<ReadRequest>,
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
                    let _ = tx.send(Self::spawn_slots(&db)).is_err();
                }
                ReadRequest::Transaction { signature, tx } => {
                    let result = match db.get_pinned_cf(
                        Rocksdb::cf_handle::<TransactionIndex>(&db),
                        TransactionIndex::encode(&signature),
                    ) {
                        Ok(Some(slice)) => {
                            TransactionIndexValue::decode(slice.as_ref(), false).map(Some)
                        }
                        Ok(None) => Ok(None),
                        Err(error) => Err(anyhow::anyhow!("failed to get tx location: {error:?}")),
                    };

                    let _ = tx.send(result).is_err();
                }
                ReadRequest::SignaturesForAddress {
                    address,
                    slot,
                    before,
                    until,
                    signatures,
                    tx,
                } => {
                    let _ = tx
                        .send(Self::spawn_signatires_for_address(
                            &db, address, slot, before, until, signatures,
                        ))
                        .is_err();
                }
                ReadRequest::SignatureStatuses { signatures, tx } => {
                    let _ = tx
                        .send(Self::spawn_signatire_statuses(&db, signatures))
                        .is_err();
                }
                ReadRequest::InflationReward {
                    epoch,
                    addresses,
                    tx,
                } => {
                    let _ = tx
                        .send(Self::spawn_inflation_reward(&db, epoch, addresses))
                        .is_err();
                }
            }
        }
    }

    fn spawn_slots(db: &DB) -> anyhow::Result<Vec<StoredBlock>> {
        let mut slots = vec![];
        let mut current_slot = None;
        for item in db.iterator_cf(
            Rocksdb::cf_handle::<SlotBasicIndex>(db),
            IteratorMode::Start,
        ) {
            let (key, value) = item.context("failed to get next item")?;
            let slot = SlotBasicIndex::decode(&key).context("failed to decode slot key")?;
            let value =
                SlotBasicIndexValue::decode(&value).context("failed to decode slot data")?;
            slots.push(StoredBlock {
                exists: true,
                dead: value.dead,
                slot,
                block_time: value.block_time,
                block_height: value.block_height,
                storage_id: value.storage_id,
                offset: value.offset,
                size: value.size,
            });

            if let Some(next_slot) = current_slot.map(|slot| slot + 1) {
                anyhow::ensure!(
                    next_slot == slot,
                    "failed to load slots from index, found a hole: {slot}, expected {next_slot}"
                );
            }
            current_slot = Some(slot);
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

    fn spawn_signatire_statuses(
        db: &DB,
        signatures: Vec<Signature>,
    ) -> anyhow::Result<Vec<(Signature, TransactionIndexValue<'static>)>> {
        let mut values = Vec::with_capacity(signatures.len());
        for signature in signatures {
            if let Some(slice) = db.get_pinned_cf(
                Rocksdb::cf_handle::<TransactionIndex>(db),
                TransactionIndex::encode(&signature),
            )? {
                values.push((
                    signature,
                    TransactionIndexValue::decode(slice.as_ref(), true)?,
                ));
            }
        }
        Ok(values)
    }

    fn spawn_inflation_reward(
        db: &DB,
        epoch: Epoch,
        addresses: Vec<Pubkey>,
    ) -> anyhow::Result<ReadRequestResultInflationReward> {
        let mut rewards =
            std::iter::repeat_n(None, addresses.len()).collect::<Vec<Option<RpcInflationReward>>>();

        let mut base_with_hasher = None;
        let mut base_fetched = false;
        let mut missed = Vec::with_capacity(addresses.len());
        for (index, address) in addresses.iter().enumerate() {
            if let Some(slice) = db.get_pinned_cf(
                Rocksdb::cf_handle::<InflationRewardIndex>(db),
                InflationRewardIndex::encode_reward(epoch, *address),
            )? {
                let value = InflationRewardAddressValue::decode(slice.as_ref())?;
                rewards[index] = Some(value.reward);
                continue;
            }

            if base_with_hasher.is_none() && !base_fetched {
                base_fetched = true;
                if let Some(slice) = db.get_pinned_cf(
                    Rocksdb::cf_handle::<InflationRewardIndex>(db),
                    InflationRewardIndex::encode_base(epoch),
                )? {
                    let base = InflationRewardBaseValue::decode(slice.as_ref())?;
                    let num_partitions_hasher =
                        if let Some(num_partitions) = base.num_reward_partitions {
                            let num_partitions = num_partitions as usize;
                            let hasher =
                                EpochRewardsHasher::new(num_partitions, &base.previous_blockhash);
                            Some((num_partitions, hasher))
                        } else {
                            None
                        };
                    base_with_hasher = Some((base, num_partitions_hasher));
                }
            }
            if let Some((base, num_partitions_hasher)) = base_with_hasher.as_ref() {
                if let Some((num_partitions, hasher)) = num_partitions_hasher {
                    let partition_index =
                        hasher.clone().hash_address_to_partition(&addresses[index]);
                    if partition_index < *num_partitions
                        && base.partitions.get(partition_index).as_deref() == Some(&true)
                    {
                        continue;
                    }
                } else {
                    continue;
                }
            }

            missed.push(index);
        }

        Ok(ReadRequestResultInflationReward {
            addresses,
            rewards,
            missed,
            base: None,
        })
    }

    pub fn read_slot_indexes(
        &self,
    ) -> anyhow::Result<BoxFuture<'static, anyhow::Result<Vec<StoredBlock>>>> {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReadRequest::Slots { tx })
            .context("failed to send ReadRequest::Slots request")?;
        Ok(Box::pin(async move {
            let ts = Instant::now();
            let slots = rx
                .await
                .context("failed to get ReadRequest::Slots request result")?;
            info!(elapsed = ?ts.elapsed(), total = ?slots.as_ref().map(|vec| vec.len()).ok(), "read slot index");
            slots
        }))
    }

    pub fn read_tx_index(
        &self,
        signature: Signature,
    ) -> anyhow::Result<BoxFuture<'static, anyhow::Result<Option<TransactionIndexValue<'static>>>>>
    {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReadRequest::Transaction { signature, tx })
            .context("failed to send ReadRequest::Transaction request")?;
        Ok(Box::pin(async move {
            rx.await
                .context("failed to get ReadRequest::Transaction request result")?
        }))
    }

    #[allow(clippy::type_complexity)]
    pub fn read_sfa_index(
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

    #[allow(clippy::type_complexity)]
    pub fn read_signature_statuses_index(
        &self,
        signatures: Vec<Signature>,
    ) -> anyhow::Result<
        BoxFuture<'static, anyhow::Result<Vec<(Signature, TransactionIndexValue<'static>)>>>,
    > {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReadRequest::SignatureStatuses { signatures, tx })
            .context("failed to send ReadRequest::SignatureStatuses request")?;
        Ok(Box::pin(async move {
            rx.await
                .context("failed to get ReadRequest::SignatureStatuses request result")?
        }))
    }

    pub fn read_inflation_reward(
        &self,
        epoch: Epoch,
        addresses: Vec<Pubkey>,
    ) -> anyhow::Result<BoxFuture<'static, anyhow::Result<ReadRequestResultInflationReward>>> {
        let (tx, rx) = oneshot::channel();
        self.req_tx
            .send(ReadRequest::InflationReward {
                epoch,
                addresses,
                tx,
            })
            .context("failed to send ReadRequest::InflationReward request")?;
        Ok(Box::pin(async move {
            rx.await
                .context("failed to get ReadRequest::InflationReward request result")?
        }))
    }
}
