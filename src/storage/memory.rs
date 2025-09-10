use {
    crate::source::block::BlockWithBinary,
    solana_sdk::clock::Slot,
    std::{collections::VecDeque, sync::Arc},
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
    const fn new(slot: Slot) -> Self {
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
    // create empty slots
    fn add_slot(&mut self, slot: Slot) -> Option<&mut BlockInfo> {
        // initialize
        if self.gen_next_slot == 0 {
            self.gen_next_slot = slot;
        }

        // drop if we already reported about that slot
        if slot < self.gen_next_slot {
            return None;
        }

        if self.blocks.is_empty() {
            self.blocks.push_back(BlockInfo::new(slot));
        } else if slot < self.blocks[0].slot {
            for slot in (slot..self.blocks[0].slot).rev() {
                self.blocks.push_front(BlockInfo::new(slot));
            }
        } else if slot > self.blocks[self.blocks.len() - 1].slot {
            for slot in self.blocks[self.blocks.len() - 1].slot + 1..=slot {
                self.blocks.push_back(BlockInfo::new(slot));
            }
        }

        let index = (slot - self.blocks[0].slot) as usize;
        Some(&mut self.blocks[index])
    }

    pub fn add_processed(&mut self, slot: Slot, block: Arc<BlockWithBinary>) {
        if let Some(item) = self.add_slot(slot) {
            item.block = Some(block);
        }
    }

    pub fn set_dead(&mut self, slot: Slot) {
        if let Some(item) = self.add_slot(slot) {
            assert!(!item.confirmed, "trying to mark confirmed slot as dead");
            item.dead = true;
        }
    }

    pub fn set_confirmed(&mut self, slot: Slot) {
        assert!(self.confirmed < slot, "attempt to backward confirmed");

        if let Some(item) = self.add_slot(slot) {
            assert!(!item.dead, "trying to mark dead slot as confirmed");
            item.confirmed = true;
            self.confirmed = slot;
        }
    }

    pub fn pop_confirmed(&mut self) -> Option<MemoryConfirmedBlock> {
        // check that confirmed & gen_next_slot is set
        if self.confirmed == 0 || self.gen_next_slot == 0 {
            return None;
        }

        // get first slot
        let first_slot = self.blocks.front().map(|b| b.slot)?;

        // get index of confirmed slot
        let mut confirmed_index = self
            .blocks
            .iter()
            .enumerate()
            .find_map(|(index, block)| block.confirmed.then_some(index))?;

        let block = loop {
            let item = &self.blocks[confirmed_index];
            match &item.block {
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
                        for index in 0..confirmed_index {
                            self.blocks[index].block = None;
                            self.blocks[index].dead = true;
                        }

                        let BlockInfo {
                            slot, block, dead, ..
                        } = self.blocks.pop_front().expect("existed");
                        break if let Some(block) = block {
                            MemoryConfirmedBlock::Block { slot, block }
                        } else {
                            MemoryConfirmedBlock::missed_or_dead(slot, dead)
                        };
                    }
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
                }
            }

            panic!(
                "failed to get next block, gen next slot = {}, first slot = {first_slot}, confirmed slot = {}, confirmed block = {}, confirmed dead = {}",
                self.gen_next_slot,
                item.slot,
                item.block.is_some(),
                item.dead,
            );
        };

        self.gen_next_slot += 1;
        Some(block)
    }
}
