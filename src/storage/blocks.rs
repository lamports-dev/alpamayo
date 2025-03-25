use {
    crate::{
        config::ConfigStorageBlocks,
        storage::{files::StorageId, slots::StoredSlots, sync::ReadWriteSyncMessage, util},
    },
    anyhow::Context,
    bitflags::bitflags,
    solana_sdk::clock::{Slot, UnixTimestamp},
    std::{collections::HashMap, io},
    thiserror::Error,
    tokio::sync::broadcast,
    tokio_uring::fs::File,
};

#[derive(Debug, Clone)]
pub struct StoredBlocksRead {
    blocks: Vec<StoredBlock>,
    tail: usize, // lowest slot
    head: usize, // highest slot
}

impl StoredBlocksRead {
    pub fn pop_block(&mut self) {
        self.blocks[self.tail] = StoredBlock::new_noexists();
        self.tail = (self.tail + 1) % self.blocks.len();
    }

    pub fn push_block(&mut self, message: StoredBlockPushSync) {
        self.head = (self.head + 1) % self.blocks.len();
        self.blocks[self.head] = message.block;
    }

    pub fn get_block_location(&self, slot: Slot) -> StorageBlockLocationResult {
        let tail = self.blocks[self.tail];
        if !tail.exists || tail.slot > slot {
            return StorageBlockLocationResult::Removed;
        }

        let head = self.blocks[self.head];
        if !head.exists || head.slot < slot {
            return StorageBlockLocationResult::NotAvailable;
        }

        let index = (self.tail + (slot - tail.slot) as usize) % self.blocks.len();
        let block = self.blocks[index];
        if block.exists && block.slot == slot {
            if block.dead {
                StorageBlockLocationResult::Dead
            } else {
                StorageBlockLocationResult::Found(block.into())
            }
        } else {
            StorageBlockLocationResult::SlotMismatch
        }
    }
}

#[derive(Debug)]
pub struct StoredBlocksWrite {
    file: File,
    blocks: Vec<StoredBlock>,
    tail: usize, // lowest slot
    head: usize, // highest slot
    stored_slots: StoredSlots,
    sync_tx: broadcast::Sender<ReadWriteSyncMessage>,
}

impl StoredBlocksWrite {
    pub async fn open(
        config: ConfigStorageBlocks,
        stored_slots: StoredSlots,
        sync_tx: broadcast::Sender<ReadWriteSyncMessage>,
    ) -> anyhow::Result<(Self, StoredBlocksRead)> {
        let (file, file_size_current) = util::open(&config.path).await?;

        // init, truncate, load, allocate
        let file_size_expected = (config.max * StoredBlock::BYTES_SIZE) as u64;
        let (blocks, tail, head) = if file_size_current == 0 {
            let blocks = Self::open_init(&file, config.max)
                .await
                .with_context(|| format!("failed to init file {:?}", config.path))?;
            let head = blocks.len() - 1;
            (blocks, 0, head)
        } else if file_size_current < file_size_expected {
            unimplemented!("truncate");
        } else if file_size_current == file_size_expected {
            Self::open_load(&file, config.max)
                .await
                .with_context(|| format!("failed to load file {:?}", config.path))?
        } else {
            unimplemented!("allocate");
        };

        let write = Self {
            file,
            blocks,
            tail,
            head,
            stored_slots: stored_slots.clone(),
            sync_tx,
        };
        stored_slots.first_available_store(write.front_slot());

        let read = StoredBlocksRead {
            blocks: write.blocks.clone(),
            tail: write.tail,
            head: write.head,
        };

        Ok((write, read))
    }

    async fn open_init(file: &File, max: usize) -> io::Result<Vec<StoredBlock>> {
        let mut buffer = vec![0; max * StoredBlock::BYTES_SIZE];
        let blocks = vec![StoredBlock::new_noexists(); max];
        for (index, block) in blocks.iter().enumerate() {
            let offset = index * StoredBlock::BYTES_SIZE;
            let bytes = &mut buffer[offset..offset + StoredBlock::BYTES_SIZE];
            block.copy_to_slice(bytes).expect("valid slice len");
        }

        let (result, _buffer) = file.write_all_at(buffer, 0).await;
        let () = result?;

        Ok(blocks)
    }

    async fn open_load(
        file: &File,
        max: usize,
    ) -> anyhow::Result<(Vec<StoredBlock>, usize, usize)> {
        let buffer = Vec::with_capacity(max * StoredBlock::BYTES_SIZE);
        let (result, buffer) = file.read_exact_at(buffer, 0).await;
        let () = result?;

        let blocks = (0..max)
            .map(|index| {
                let offset = index * StoredBlock::BYTES_SIZE;
                let bytes = &buffer[offset..offset + StoredBlock::BYTES_SIZE];
                StoredBlock::from_bytes(bytes)
            })
            .collect::<Result<Vec<_>, _>>()
            .context("failed to parse block header")?;

        let iter = blocks
            .iter()
            .enumerate()
            .filter(|(_index, block)| block.exists && !block.dead);

        let tail = iter
            .clone()
            .min_by_key(|(_index, block)| block.slot)
            .map(|(index, _block)| index)
            .unwrap_or_default();

        let head = iter
            .max_by_key(|(_index, block)| block.slot)
            .map(|(index, _block)| index)
            .unwrap_or_else(|| blocks.len() - 1);

        Ok((blocks, tail, head))
    }

    pub async fn close(self) {
        let _: io::Result<()> = self.file.close().await;
    }

    pub fn get_stored_boundaries(&self) -> HashMap<StorageId, StorageBlocksBoundaries> {
        let mut map = HashMap::<StorageId, StorageBlocksBoundaries>::new();
        for block in self.blocks.iter() {
            if block.exists && !block.dead {
                map.entry(block.storage_id).or_default().update(block);
            }
        }
        map
    }

    pub fn is_full(&self) -> bool {
        let next = (self.head + 1) % self.blocks.len();
        self.blocks[next].exists
    }

    pub fn get_latest_slot(&self) -> Option<Slot> {
        let block = self.blocks[self.head];
        block.exists.then_some(block.slot)
    }

    async fn sync(&self, index: usize) -> io::Result<()> {
        let mut buffer = vec![0; StoredBlock::BYTES_SIZE];
        self.blocks[index]
            .copy_to_slice(&mut buffer)
            .expect("valid slice len");

        let (result, _buffer) = self
            .file
            .write_all_at(buffer, (index * StoredBlock::BYTES_SIZE) as u64)
            .await;
        let () = result?;
        self.file.sync_data().await?;

        Ok(())
    }

    async fn push_block(&mut self, block: StoredBlock) -> anyhow::Result<()> {
        self.head = (self.head + 1) % self.blocks.len();
        anyhow::ensure!(!self.blocks[self.head].exists, "no free slot");

        let _ = self.sync_tx.send(ReadWriteSyncMessage::ConfirmedBlockPush {
            block: block.into(),
        });

        self.blocks[self.head] = block;
        self.sync(self.head)
            .await
            .map_err(|error| anyhow::anyhow!("failed to sync block headers: {error:?}"))?;

        // update stored if db was initialized
        if self.tail == 0 && self.head == 0 {
            self.stored_slots.first_available_store(self.front_slot());
        }

        Ok(())
    }

    pub async fn push_block_dead(&mut self, slot: Slot) -> anyhow::Result<()> {
        self.push_block(StoredBlock::new_dead(slot)).await
    }

    pub async fn push_block_confirmed(
        &mut self,
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        storage_id: StorageId,
        offset: u64,
        block_size: u64,
    ) -> anyhow::Result<()> {
        self.push_block(StoredBlock::new_confirmed(
            slot, block_time, storage_id, offset, block_size,
        ))
        .await
    }

    pub async fn pop_block(&mut self) -> io::Result<Option<StorageBlockLocation>> {
        if !self.blocks[self.tail].exists {
            return Ok(None);
        }

        let _ = self.sync_tx.send(ReadWriteSyncMessage::ConfirmedBlockPop);

        let block = std::mem::replace(&mut self.blocks[self.tail], StoredBlock::new_noexists());

        self.stored_slots.first_available_store(self.front_slot());

        self.sync(self.tail).await?;

        self.tail = (self.tail + 1) % self.blocks.len();

        Ok(Some(block.into()))
    }

    fn front_slot(&self) -> Option<Slot> {
        if self.tail == 0 && self.head == self.blocks.len() - 1 {
            return None;
        }

        let mut index = self.tail;
        loop {
            let block = &self.blocks[index];
            if block.exists {
                return Some(block.slot);
            }
            index = (index + 1) % self.blocks.len();
            if index == self.head {
                break;
            }
        }
        None
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct StoredBlock {
    exists: bool,
    dead: bool,
    slot: Slot,
    block_time: Option<UnixTimestamp>,
    storage_id: StorageId,
    offset: u64,
    size: u64,
}

impl StoredBlock {
    // flags: u32
    // storage_id: u32
    // slot: u64
    // block_time: i64
    // offset: u64
    // size: u64
    // total: 40 bytes
    const BYTES_SIZE: usize = 64;

    fn new_noexists() -> Self {
        Self::default()
    }

    fn new_dead(slot: Slot) -> Self {
        Self {
            exists: true,
            dead: true,
            slot,
            ..Default::default()
        }
    }

    fn new_confirmed(
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        storage_id: StorageId,
        offset: u64,
        size: u64,
    ) -> Self {
        Self {
            exists: true,
            dead: false,
            slot,
            block_time,
            storage_id,
            offset,
            size,
        }
    }

    fn from_bytes(bytes: &[u8]) -> Result<Self, StoredBlockParseError> {
        if bytes.len() != Self::BYTES_SIZE {
            return Err(StoredBlockParseError::InvalidLength(bytes.len()));
        }

        let bits = u32::from_be_bytes(bytes[0..4].try_into()?);
        let Some(flags) = StoredBlockFlags::from_bits(bits) else {
            return Err(StoredBlockParseError::InvalidBits(bits));
        };

        Ok(Self {
            exists: flags.contains(StoredBlockFlags::EXISTS),
            dead: flags.contains(StoredBlockFlags::DEAD),
            slot: u64::from_be_bytes(bytes[8..16].try_into()?),
            block_time: flags
                .contains(StoredBlockFlags::BLOCK_TIME)
                .then_some(UnixTimestamp::from_be_bytes(bytes[16..24].try_into()?)),
            storage_id: StorageId::from_be_bytes(bytes[4..8].try_into()?),
            offset: u64::from_be_bytes(bytes[24..32].try_into()?),
            size: u64::from_be_bytes(bytes[32..40].try_into()?),
        })
    }

    fn copy_to_slice(self, bytes: &mut [u8]) -> Result<(), StoredBlocksParseError> {
        if bytes.len() != Self::BYTES_SIZE {
            return Err(StoredBlocksParseError::InvalidLength(bytes.len()));
        }

        // flags: u32
        let mut flags = StoredBlockFlags::empty();
        if self.exists {
            flags |= StoredBlockFlags::EXISTS;
        }
        if self.dead {
            flags |= StoredBlockFlags::DEAD;
        }
        if self.block_time.is_some() {
            flags |= StoredBlockFlags::BLOCK_TIME;
        }
        bytes[0..4].copy_from_slice(&flags.bits().to_be_bytes()[..]);

        // storage_id: u32
        bytes[4..8].copy_from_slice(&self.storage_id.to_be_bytes()[..]);

        // slot: u64
        bytes[8..16].copy_from_slice(&self.slot.to_be_bytes()[..]);

        // block_time: u64
        if let Some(block_time) = self.block_time {
            bytes[16..24].copy_from_slice(&block_time.to_be_bytes()[..]);
        }

        // offset: u64
        bytes[24..32].copy_from_slice(&self.offset.to_be_bytes()[..]);

        // size: u64
        bytes[32..40].copy_from_slice(&self.size.to_be_bytes()[..]);

        Ok(())
    }
}

#[derive(Debug, Error)]
enum StoredBlockParseError {
    #[error("slice length is invalid: {0} (expected 64)")]
    InvalidLength(usize),
    #[error("invalid bits: {0}")]
    InvalidBits(u32),
    #[error(transparent)]
    TryFromSliceError(#[from] std::array::TryFromSliceError),
}

#[derive(Debug, Error)]
enum StoredBlocksParseError {
    #[error("slice length is invalid: {0} (expected 64)")]
    InvalidLength(usize),
}

bitflags! {
    struct StoredBlockFlags: u32 {
        const EXISTS =     0b00000001;
        const DEAD =       0b00000010;
        const BLOCK_TIME = 0b00000100;
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StorageBlocksBoundaries {
    min: Option<StoredBlock>,
    max: Option<StoredBlock>,
}

impl StorageBlocksBoundaries {
    fn update(&mut self, block: &StoredBlock) {
        if let Some(min) = &mut self.min {
            if block.slot < min.slot {
                *min = *block;
            }
        } else {
            self.min = Some(*block);
        }

        if let Some(max) = &mut self.max {
            if block.slot > max.slot {
                *max = *block;
            }
        } else {
            self.max = Some(*block);
        }
    }

    pub fn tail(&self) -> Option<u64> {
        self.min.map(|block| block.offset)
    }

    pub fn head(&self) -> Option<u64> {
        self.max.map(|block| block.offset + block.size)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct StorageBlockLocation {
    pub slot: Slot,
    pub storage_id: StorageId,
    pub offset: u64,
    pub size: u64,
}

impl From<StoredBlock> for StorageBlockLocation {
    fn from(block: StoredBlock) -> Self {
        Self {
            slot: block.slot,
            storage_id: block.storage_id,
            offset: block.offset,
            size: block.size,
        }
    }
}

#[derive(Debug)]
pub enum StorageBlockLocationResult {
    Removed,      // block is not available anymore
    Dead,         // skipped or forked block for this slot
    NotAvailable, // not confirmed yet
    SlotMismatch,
    Found(StorageBlockLocation),
}

#[derive(Debug, Clone, Copy)]
pub struct StoredBlockPushSync {
    block: StoredBlock,
}

impl From<StoredBlock> for StoredBlockPushSync {
    fn from(block: StoredBlock) -> Self {
        Self { block }
    }
}

impl StoredBlockPushSync {
    pub fn slot(&self) -> Slot {
        self.block.slot
    }
}
