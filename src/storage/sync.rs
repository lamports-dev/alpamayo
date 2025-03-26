use {
    crate::{
        source::block::BlockWithBinary,
        storage::{
            blocks::{StoredBlockPushSync, StoredBlocksRead},
            files::StorageFilesSyncInit,
        },
    },
    solana_sdk::clock::Slot,
};

#[derive(Debug, Clone)]
pub enum ReadWriteSyncMessage {
    // once, on initialization
    Init {
        blocks: StoredBlocksRead,
        storage_files_init: StorageFilesSyncInit,
    },
    // when we build the block
    BlockNew {
        slot: Slot,
        block: BlockWithBinary,
    },
    // block marked as dead
    BlockDead {
        slot: Slot,
    },
    // block confirmed
    BlockConfirmed {
        slot: Slot,
        block: Option<BlockWithBinary>,
    },
    // confirmed/finalized block removed from the storage
    ConfirmedBlockPop,
    // confirmed block added to storage
    ConfirmedBlockPush {
        block: StoredBlockPushSync,
    },
}
