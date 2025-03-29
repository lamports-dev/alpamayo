use {
    crate::{
        source::block::BlockWithBinary,
        storage::{
            blocks::{StoredBlockPushSync, StoredBlocksRead},
            files::StorageFilesSyncInit,
            rocksdb::RocksdbRead,
        },
    },
    solana_sdk::clock::Slot,
    std::sync::Arc,
};

#[derive(Debug, Clone)]
pub enum ReadWriteSyncMessage {
    // once, on initialization
    Init {
        blocks: StoredBlocksRead,
        db_read: RocksdbRead,
        storage_files_init: StorageFilesSyncInit,
    },
    // when we build the block
    BlockNew {
        slot: Slot,
        block: Arc<BlockWithBinary>,
    },
    // block marked as dead
    BlockDead {
        slot: Slot,
    },
    // block confirmed
    BlockConfirmed {
        slot: Slot,
        block: Option<Arc<BlockWithBinary>>,
    },
    SlotFinalized {
        slot: Slot,
    },
    // confirmed/finalized block removed from the storage
    ConfirmedBlockPop,
    // confirmed block added to storage
    ConfirmedBlockPush {
        block: StoredBlockPushSync,
    },
}
