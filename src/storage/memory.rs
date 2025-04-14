use {
    crate::source::block::BlockWithBinary,
    solana_sdk::clock::Slot,
    std::{collections::VecDeque, sync::Arc},
    tracing::error,
};

#[derive(Debug)]
pub enum MemoryConfirmedBlock {
    // we don't received info about that block
    Missed {
        slot: Slot,
    },
    // block is dead
    Dead {
        slot: Slot,
    },
    // constructed block
    Block {
        slot: Slot,
        block: Arc<BlockWithBinary>,
    },
}

impl MemoryConfirmedBlock {
    const fn missed_or_dead(slot: Slot, dead: bool) -> Self {
        if dead {
            Self::Dead { slot }
        } else {
            Self::Missed { slot }
        }
    }

    pub const fn get_slot(&self) -> Slot {
        *match self {
            Self::Missed { slot } => slot,
            Self::Dead { slot } => slot,
            Self::Block { slot, .. } => slot,
        }
    }
}

#[derive(Debug)]
struct BlockInfo {
    slot: Slot,
    block: Option<Arc<BlockWithBinary>>,
    dead: bool,
    confirmed: bool,
}

impl BlockInfo {
    const fn new(slot: Slot, block: Arc<BlockWithBinary>) -> Self {
        Self {
            slot,
            block: Some(block),
            dead: false,
            confirmed: false,
        }
    }

    const fn new_empty(slot: Slot) -> Self {
        Self {
            slot,
            block: None,
            dead: false,
            confirmed: false,
        }
    }
}

#[derive(Debug, Default)]
pub struct StorageMemory {
    blocks: VecDeque<BlockInfo>,
    confirmed: Slot,
    gen_next_slot: Slot,
}

impl StorageMemory {
    pub fn add_processed(&mut self, slot: Slot, block: Arc<BlockWithBinary>) {
        // drop if we already reported about that slot
        if slot < self.gen_next_slot {
            return;
        }

        // add slot directly if we don't have blocks
        if self.blocks.is_empty() {
            self.blocks.push_back(BlockInfo::new(slot, block));
            return;
        }

        // push empty slots
        if slot < self.blocks[0].slot {
            for slot in (slot..self.blocks[0].slot).rev() {
                self.blocks.push_front(BlockInfo::new_empty(slot));
            }
        } else if slot > self.blocks[self.blocks.len() - 1].slot {
            for slot in self.blocks[self.blocks.len() - 1].slot + 1..=slot {
                self.blocks.push_back(BlockInfo::new_empty(slot));
            }
        }

        // update block
        let index = (slot - self.blocks[0].slot) as usize;
        self.blocks[index].block = Some(block);
    }

    pub fn set_dead(&mut self, slot: Slot) {
        if let Some(first_block) = self.blocks.front() {
            if first_block.slot <= slot {
                let index = (slot - first_block.slot) as usize;
                if let Some(info) = self.blocks.get_mut(index) {
                    info.dead = true;
                }
            }
        }
    }

    pub fn set_confirmed(&mut self, slot: Slot) {
        assert!(self.confirmed < slot, "attempt to backward confirmed");
        if let Some(first_block) = self.blocks.front() {
            if first_block.slot <= slot {
                let index = (slot - first_block.slot) as usize;
                if let Some(info) = self.blocks.get_mut(index) {
                    info.confirmed = true;
                }
            }
        }
        self.confirmed = slot;
    }

    pub fn pop_confirmed(&mut self) -> Option<MemoryConfirmedBlock> {
        // check that confirmed is set
        if self.confirmed == 0 {
            return None;
        }

        // get first slot
        let first_slot = self.blocks.front().map(|b| b.slot)?;

        // initialize gen_next_slot
        if self.gen_next_slot == 0 {
            self.gen_next_slot = self.confirmed.min(first_slot);
        }

        // we don't have slots yet
        if self.confirmed < first_slot {
            return None;
        }

        let mut confirmed_index = self
            .blocks
            .iter()
            .enumerate()
            .find_map(|(index, block)| block.confirmed.then_some(index))?;

        let block = loop {
            match &self.blocks[confirmed_index].block {
                Some(block) => {
                    // update confirmed index
                    if first_slot <= block.parent_slot {
                        confirmed_index = (block.parent_slot - first_slot) as usize;
                        continue;
                    }

                    // we don't have info about block
                    if self.gen_next_slot <= block.parent_slot {
                        let slot = self.gen_next_slot;
                        break MemoryConfirmedBlock::Missed { slot };
                    }

                    // missed slots
                    if self.gen_next_slot < first_slot {
                        let slot = self.gen_next_slot;
                        break MemoryConfirmedBlock::Dead { slot };
                    }

                    // missed if not marked as dead
                    if self.gen_next_slot == first_slot {
                        let BlockInfo {
                            slot, block, dead, ..
                        } = self.blocks.pop_front().expect("existed");
                        break if let Some(block) = block {
                            MemoryConfirmedBlock::Block { slot, block }
                        } else {
                            MemoryConfirmedBlock::missed_or_dead(slot, dead)
                        };
                    }

                    error!(
                        gen_next_slot = self.gen_next_slot,
                        "unexpected gen_next_slot for confirmed_index, 1"
                    );
                }
                None => {
                    // we don't have any info, definitely missed
                    if self.gen_next_slot < first_slot {
                        let slot = self.gen_next_slot;
                        break MemoryConfirmedBlock::Missed { slot };
                    }

                    // missed if not marked as dead
                    if self.gen_next_slot == first_slot {
                        let BlockInfo { slot, dead, .. } =
                            self.blocks.pop_front().expect("existed");
                        break MemoryConfirmedBlock::missed_or_dead(slot, dead);
                    }

                    error!(
                        gen_next_slot = self.gen_next_slot,
                        "unexpected gen_next_slot for confirmed_index, 2"
                    );
                }
            }

            return None;
        };

        self.gen_next_slot += 1;
        Some(block)
    }
}
