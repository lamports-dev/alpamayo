use {
    crate::{
        rpc::api_jsonrpc::RpcRequestBlocksUntil,
        storage::{files::StorageId, slots::StoredSlots, sync::ReadWriteSyncMessage},
        util::HashMap,
    },
    solana_sdk::clock::{MAX_RECENT_BLOCKHASHES, Slot, UnixTimestamp},
    tokio::sync::broadcast,
};

#[derive(Debug)]
pub struct StoredBlocksWrite {
    blocks: Vec<StoredBlock>,
    tail: usize, // lowest slot
    head: usize, // highest slot
    stored_slots: StoredSlots,
    sync_tx: broadcast::Sender<ReadWriteSyncMessage>,
}

impl StoredBlocksWrite {
    pub fn new(
        mut blocks: Vec<StoredBlock>,
        max: usize,
        stored_slots: StoredSlots,
        sync_tx: broadcast::Sender<ReadWriteSyncMessage>,
    ) -> anyhow::Result<Self> {
        anyhow::ensure!(
            blocks.len() <= max,
            "shrinking of stored blocks is not supported yet"
        );

        blocks.resize(max, StoredBlock::new_noexists());

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

        let this = Self {
            blocks,
            tail,
            head,
            stored_slots: stored_slots.clone(),
            sync_tx,
        };

        stored_slots.first_available_store(this.get_first_slot());

        Ok(this)
    }

    pub fn get_recent_blocks(&self) -> Vec<StoredBlock> {
        let mut blocks = Vec::with_capacity(MAX_RECENT_BLOCKHASHES);

        let mut index = self.head;
        while blocks.len() < MAX_RECENT_BLOCKHASHES
            && index != self.tail
            && self.blocks[index].exists
        {
            if !self.blocks[index].dead {
                blocks.push(self.blocks[index]);
            }

            index = if index == 0 {
                self.blocks.len() - 1
            } else {
                index - 1
            };
        }

        blocks.reverse();
        blocks
    }

    pub fn to_read(&self) -> StoredBlocksRead {
        StoredBlocksRead {
            blocks: self.blocks.clone(),
            tail: self.tail,
            head: self.head,
        }
    }

    pub fn is_full(&self) -> bool {
        let next = (self.head + 1) % self.blocks.len();
        self.blocks[next].exists
    }

    pub fn get_stored_boundaries(&self) -> HashMap<StorageId, StorageBlocksBoundaries> {
        let mut map = HashMap::<StorageId, StorageBlocksBoundaries>::default();
        for block in self.blocks.iter() {
            if block.exists && !block.dead {
                map.entry(block.storage_id).or_default().update(block);
            }
        }
        map
    }

    pub fn get_latest_slot(&self) -> Option<Slot> {
        let block = self.blocks[self.head];
        block.exists.then_some(block.slot)
    }

    pub fn get_latest_height(&self) -> Option<Slot> {
        let mut index = self.head;
        loop {
            let block = self.blocks[index];
            if block.exists && !block.dead {
                return block.block_height;
            }
            if index == self.tail {
                return None;
            }
            index = index.checked_sub(1).unwrap_or(self.blocks.len() - 1);
        }
    }

    pub fn push_block_front_dead(&mut self, slot: Slot) -> anyhow::Result<()> {
        self.push_block_front2(StoredBlock::new_dead(slot))
    }

    pub fn push_block_front_confirmed(
        &mut self,
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        block_height: Option<Slot>,
        storage_id: StorageId,
        offset: u64,
        block_size: u64,
    ) -> anyhow::Result<()> {
        self.push_block_front2(StoredBlock::new_confirmed(
            slot,
            block_time,
            block_height,
            storage_id,
            offset,
            block_size,
        ))
    }

    fn push_block_front2(&mut self, block: StoredBlock) -> anyhow::Result<()> {
        self.head = (self.head + 1) % self.blocks.len();
        anyhow::ensure!(!self.blocks[self.head].exists, "no free slot");

        let _ = self.sync_tx.send(ReadWriteSyncMessage::ConfirmedBlockPush {
            block: block.into(),
        });

        self.blocks[self.head] = block;
        self.update_total(false);
        Ok(())
    }

    fn get_first(&self, filter: impl Fn(&StoredBlock) -> bool) -> Option<&StoredBlock> {
        // additional condition in case if zero blocks exists
        if self.blocks[self.tail].exists && self.blocks[self.head].exists {
            let mut index = self.tail;
            loop {
                let block = &self.blocks[index];
                if filter(block) {
                    return Some(block);
                }
                if index == self.head {
                    break;
                }
                index = (index + 1) % self.blocks.len();
            }
        }
        None
    }

    pub fn get_first_slot(&self) -> Option<Slot> {
        self.get_first(|blk| blk.exists).map(|blk| blk.slot)
    }

    pub fn get_first_height(&self) -> Option<Slot> {
        self.get_first(|blk| blk.exists && !blk.dead)
            .and_then(|blk| blk.block_height)
    }

    pub fn push_block_back_dead(&mut self, slot: Slot) -> anyhow::Result<()> {
        self.push_block_back2(StoredBlock::new_dead(slot))
    }

    pub fn push_block_back_confirmed(
        &mut self,
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        block_height: Option<Slot>,
        storage_id: StorageId,
        offset: u64,
        block_size: u64,
    ) -> anyhow::Result<()> {
        self.push_block_back2(StoredBlock::new_confirmed(
            slot,
            block_time,
            block_height,
            storage_id,
            offset,
            block_size,
        ))
    }

    fn push_block_back2(&mut self, block: StoredBlock) -> anyhow::Result<()> {
        self.tail = self.tail.checked_sub(1).unwrap_or(self.blocks.len() - 1);
        anyhow::ensure!(!self.blocks[self.tail].exists, "no free slot");

        self.blocks[self.tail] = block;
        self.update_total(true);
        Ok(())
    }

    fn update_total(&self, push_back: bool) {
        let total = if self.head >= self.tail {
            self.head - self.tail + 1
        } else {
            (self.blocks.len() - self.tail) + self.head + 1
        };
        self.stored_slots.set_total(total);

        // update stored if db was initialized
        if push_back || (self.tail == 0 && self.head == 0) {
            self.stored_slots
                .first_available_store(self.get_first_slot());
        }
    }

    pub fn pop_block(&mut self) -> Option<StoredBlock> {
        if self.blocks[self.tail].exists {
            let block = std::mem::replace(&mut self.blocks[self.tail], StoredBlock::new_noexists());
            self.tail = (self.tail + 1) % self.blocks.len();
            self.stored_slots
                .first_available_store(self.get_first_slot());
            Some(block)
        } else {
            None
        }
    }
}

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
                StorageBlockLocationResult::Found(block)
            }
        } else {
            StorageBlockLocationResult::SlotMismatch
        }
    }

    pub fn get_blocks(
        &self,
        start_slot: Slot,
        end_slot: Slot,
        until: RpcRequestBlocksUntil,
    ) -> anyhow::Result<Vec<Slot>> {
        let tail = self.blocks[self.tail];
        anyhow::ensure!(
            tail.exists && tail.slot <= start_slot,
            "requested start slot removed"
        );

        let head = self.blocks[self.head];
        anyhow::ensure!(
            head.exists && head.slot >= end_slot,
            "end slot out of limit"
        );

        let mut blocks = Vec::with_capacity(match until {
            RpcRequestBlocksUntil::EndSlot(end_slot) => (end_slot - start_slot) as usize,
            RpcRequestBlocksUntil::Limit(limit) => limit,
        });

        let mut index = (self.tail + (start_slot - tail.slot) as usize) % self.blocks.len();
        loop {
            let block = self.blocks[index];
            if !block.exists {
                break;
            }
            if block.dead {
                continue;
            }

            if let RpcRequestBlocksUntil::Limit(limit) = until {
                if blocks.len() == limit {
                    break;
                }
            }

            blocks.push(block.slot);
            index = (index + 1) % self.blocks.len();

            if let RpcRequestBlocksUntil::EndSlot(end_slot) = until {
                if end_slot == block.slot {
                    break;
                }
            }
        }

        Ok(blocks)
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StoredBlock {
    pub exists: bool,
    pub dead: bool,
    pub slot: Slot,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<Slot>,
    pub storage_id: StorageId,
    pub offset: u64,
    pub size: u64,
}

impl StoredBlock {
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

    const fn new_confirmed(
        slot: Slot,
        block_time: Option<UnixTimestamp>,
        block_height: Option<Slot>,
        storage_id: StorageId,
        offset: u64,
        size: u64,
    ) -> Self {
        Self {
            exists: true,
            dead: false,
            slot,
            block_time,
            block_height,
            storage_id,
            offset,
            size,
        }
    }
}

#[derive(Debug, Default, Clone, Copy)]
pub struct StorageBlocksBoundaries {
    min: Option<StoredBlock>,
    max: Option<StoredBlock>,
}

impl StorageBlocksBoundaries {
    const fn update(&mut self, block: &StoredBlock) {
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

#[derive(Debug)]
pub enum StorageBlockLocationResult {
    Removed,      // block is not available anymore
    Dead,         // skipped or forked block for this slot
    NotAvailable, // not confirmed yet
    SlotMismatch,
    Found(StoredBlock),
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
    pub const fn slot(&self) -> Slot {
        self.block.slot
    }
}
