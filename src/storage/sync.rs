use crate::storage::{
    blocks::{StoredBlockPushSync, StoredBlocksRead},
    files::StorageFilesSyncInit,
};

#[derive(Debug, Clone)]
pub enum ReadWriteSyncMessage {
    Init {
        blocks: StoredBlocksRead,
        storage_files_init: StorageFilesSyncInit,
    },
    BlockPop,
    BlockPush {
        block: StoredBlockPushSync,
    },
}
