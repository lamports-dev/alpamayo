use {
    crate::{
        config::ConfigStorageBlocks,
        storage::{files::StorageId, util},
    },
    anyhow::Context,
    bitflags::bitflags,
    solana_sdk::clock::{Slot, UnixTimestamp},
    std::{collections::HashMap, io},
    thiserror::Error,
    tokio_uring::fs::File,
};

#[derive(Debug)]
pub struct StoredBlockHeaders {
    file: File,
    blocks: Vec<StoredBlockHeader>,
    head: usize,
}

impl StoredBlockHeaders {
    pub async fn open(config: ConfigStorageBlocks) -> anyhow::Result<Self> {
        let (file, file_size_current) = util::open(&config.path).await?;

        // init, truncate, load, allocate
        let file_size_expected = (config.max * StoredBlockHeader::BYTES_SIZE) as u64;
        let (blocks, head) = if file_size_current == 0 {
            let blocks = Self::open_init(&file, config.max)
                .await
                .with_context(|| format!("failed to init file {:?}", config.path))?;
            (blocks, 0)
        } else if file_size_current < file_size_expected {
            unimplemented!("truncate");
        } else if file_size_current == file_size_expected {
            Self::open_load(&file, config.max)
                .await
                .with_context(|| format!("failed to load file {:?}", config.path))?
        } else {
            unimplemented!("allocate");
        };

        Ok(Self { file, blocks, head })
    }

    async fn open_init(file: &File, max: usize) -> io::Result<Vec<StoredBlockHeader>> {
        let mut buffer = vec![0; max * StoredBlockHeader::BYTES_SIZE];
        let blocks = vec![StoredBlockHeader::default(); max];
        for (index, block) in blocks.iter().enumerate() {
            let offset = index * StoredBlockHeader::BYTES_SIZE;
            let bytes = &mut buffer[offset..offset + StoredBlockHeader::BYTES_SIZE];
            block.copy_to_slice(bytes).expect("valid slice len");
        }

        let (result, _buffer) = file.write_all_at(buffer, 0).await;
        let () = result?;

        Ok(blocks)
    }

    async fn open_load(file: &File, max: usize) -> anyhow::Result<(Vec<StoredBlockHeader>, usize)> {
        let buffer = Vec::with_capacity(max * StoredBlockHeader::BYTES_SIZE);
        let (result, buffer) = file.read_exact_at(buffer, 0).await;
        let () = result?;

        let blocks = (0..max)
            .map(|index| {
                let offset = index * StoredBlockHeader::BYTES_SIZE;
                let bytes = &buffer[offset..offset + StoredBlockHeader::BYTES_SIZE];
                StoredBlockHeader::from_bytes(bytes)
            })
            .collect::<Result<Vec<_>, _>>()
            .context("failed to parse block header")?;

        let head = blocks
            .iter()
            .enumerate()
            .filter(|(_index, block)| block.exists && !block.dead)
            .max_by_key(|(_index, block)| block.slot)
            .map(|(index, _block)| index)
            .unwrap_or_default();
        Ok((blocks, head))
    }

    pub async fn close(self) {
        let _: io::Result<()> = self.file.close().await;
    }

    pub fn get_stored_boundaries(&self) -> HashMap<StorageId, StorageBlockBoundaries> {
        let mut map = HashMap::<StorageId, StorageBlockBoundaries>::new();
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
        let mut buffer = vec![0; StoredBlockHeader::BYTES_SIZE];
        self.blocks[index]
            .copy_to_slice(&mut buffer)
            .expect("valid slice len");

        let (result, _buffer) = self
            .file
            .write_all_at(buffer, (index * StoredBlockHeader::BYTES_SIZE) as u64)
            .await;
        let () = result?;

        Ok(())
    }

    async fn push_block(&mut self, block: StoredBlockHeader) -> anyhow::Result<()> {
        self.head = (self.head + 1) % self.blocks.len();
        anyhow::ensure!(
            !self.blocks[self.head].exists,
            "blocks should have free slot"
        );
        self.blocks[self.head] = block;
        self.sync(self.head)
            .await
            .map_err(|error| anyhow::anyhow!("failed to sync block headers: {error:?}"))
    }

    pub async fn push_block_dead(&mut self, slot: Slot) -> anyhow::Result<()> {
        self.push_block(StoredBlockHeader::new_dead(slot)).await
    }

    pub async fn push_block_confirmed(
        &mut self,
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        storage_id: StorageId,
        offset: u64,
        block_size: u64,
    ) -> anyhow::Result<()> {
        self.push_block(StoredBlockHeader::new_confirmed(
            slot, block_time, storage_id, offset, block_size,
        ))
        .await
    }
}

#[derive(Debug, Default, Clone, Copy)]
struct StoredBlockHeader {
    exists: bool,
    dead: bool,
    slot: Slot,
    block_time: Option<UnixTimestamp>,
    storage_id: StorageId,
    offset: u64,
    size: u64,
}

impl StoredBlockHeader {
    // flags: u32
    // storage_id: u32
    // slot: u64
    // block_time: i64
    // offset: u64
    // size: u64
    // total: 40 bytes
    const BYTES_SIZE: usize = 64;

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

    fn from_bytes(bytes: &[u8]) -> Result<Self, StoredBlockHeaderParseError> {
        if bytes.len() != Self::BYTES_SIZE {
            return Err(StoredBlockHeaderParseError::InvalidLength(bytes.len()));
        }

        let bits = u32::from_be_bytes(bytes[0..4].try_into()?);
        let Some(flags) = StoredBlockHeaderFlags::from_bits(bits) else {
            return Err(StoredBlockHeaderParseError::InvalidBits(bits));
        };

        Ok(Self {
            exists: flags.contains(StoredBlockHeaderFlags::EXISTS),
            dead: flags.contains(StoredBlockHeaderFlags::DEAD),
            slot: u64::from_be_bytes(bytes[8..16].try_into()?),
            block_time: flags
                .contains(StoredBlockHeaderFlags::BLOCK_TIME)
                .then_some(UnixTimestamp::from_be_bytes(bytes[16..24].try_into()?)),
            storage_id: StorageId::from_be_bytes(bytes[4..8].try_into()?),
            offset: u64::from_be_bytes(bytes[24..32].try_into()?),
            size: u64::from_be_bytes(bytes[32..40].try_into()?),
        })
    }

    fn copy_to_slice(self, bytes: &mut [u8]) -> Result<(), StoredBlockHeaderSerError> {
        if bytes.len() != Self::BYTES_SIZE {
            return Err(StoredBlockHeaderSerError::InvalidLength(bytes.len()));
        }

        // flags: u32
        let mut flags = StoredBlockHeaderFlags::empty();
        if self.exists {
            flags |= StoredBlockHeaderFlags::EXISTS;
        }
        if self.dead {
            flags |= StoredBlockHeaderFlags::DEAD;
        }
        if self.block_time.is_some() {
            flags |= StoredBlockHeaderFlags::BLOCK_TIME;
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
enum StoredBlockHeaderParseError {
    #[error("slice length is invalid: {0} (expected 64)")]
    InvalidLength(usize),
    #[error("invalid bits: {0}")]
    InvalidBits(u32),
    #[error(transparent)]
    TryFromSliceError(#[from] std::array::TryFromSliceError),
}

#[derive(Debug, Error)]
enum StoredBlockHeaderSerError {
    #[error("slice length is invalid: {0} (expected 64)")]
    InvalidLength(usize),
}

bitflags! {
    struct StoredBlockHeaderFlags: u32 {
        const EXISTS =     0b00000001;
        const DEAD =       0b00000010;
        const BLOCK_TIME = 0b00000100;
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StorageBlockBoundaries {
    min: Option<StoredBlockHeader>,
    max: Option<StoredBlockHeader>,
}

impl StorageBlockBoundaries {
    fn update(&mut self, block: &StoredBlockHeader) {
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
