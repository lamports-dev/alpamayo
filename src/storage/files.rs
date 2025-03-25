use {
    crate::{
        config::ConfigStorageFile,
        source::block::ConfirmedBlockWithBinary,
        storage::{blocks::StoredBlocksWrite, util},
    },
    anyhow::Context,
    futures::future::{FutureExt, LocalBoxFuture, TryFutureExt, join_all, try_join_all},
    solana_sdk::clock::Slot,
    std::{collections::HashMap, io, path::PathBuf, rc::Rc},
    tokio_uring::fs::File,
};

pub type StorageId = u32;

#[derive(Debug)]
pub struct StorageFilesRead {
    files: Vec<Rc<File>>,
    id2file: HashMap<StorageId, usize>,
}

impl StorageFilesRead {
    pub async fn open(config: StorageFilesSyncInit) -> anyhow::Result<Self> {
        let files = try_join_all(
            config
                .files_paths
                .iter()
                .map(|path| util::open(path).map_ok(|(file, _file_size)| Rc::new(file))),
        )
        .await?;

        Ok(Self {
            files,
            id2file: config.id2file,
        })
    }

    pub fn read<'a>(
        &self,
        storage_id: StorageId,
        offset: u64,
        size: u64,
    ) -> LocalBoxFuture<'a, io::Result<Vec<u8>>> {
        let file = self
            .id2file
            .get(&storage_id)
            .and_then(|index| self.files.get(*index))
            .map(Rc::clone);

        async move {
            let Some(file) = file else {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("failed to get file for id#{storage_id}"),
                ));
            };

            let buffer = Vec::with_capacity(size as usize);
            let (res, buffer) = file.read_exact_at(buffer, offset).await;
            res?;

            Ok(buffer)
        }
        .boxed_local()
    }
}

#[derive(Debug)]
pub struct StorageFilesWrite {
    files: Vec<StorageFile>,
    id2file: HashMap<StorageId, usize>,
    next_file: usize,
}

impl StorageFilesWrite {
    pub async fn open(
        configs: Vec<ConfigStorageFile>,
        blocks: &StoredBlocksWrite,
    ) -> anyhow::Result<(Self, StorageFilesSyncInit)> {
        let files_paths = configs.iter().map(|config| config.path.clone()).collect();
        let mut files = try_join_all(configs.into_iter().map(Self::open_file)).await?;
        files.sort_unstable_by_key(|file| file.id);

        // storage id map
        let mut id2file = HashMap::new();
        for (index, file) in files.iter().enumerate() {
            id2file.insert(file.id, index);
        }

        // set tail and head
        let mut boundaries = blocks.get_stored_boundaries();
        for (storage_id, index) in id2file.iter() {
            if let Some(boundaries) = boundaries.remove(storage_id) {
                let file = &mut files[*index];
                file.tail = boundaries.tail().unwrap_or_default();
                anyhow::ensure!(
                    file.tail < file.size,
                    "invalid tail for file id#{}",
                    file.id
                );
                file.head = boundaries.head().unwrap_or_default();
                anyhow::ensure!(file.head <= file.size, "invalid head for id#{}", file.id);
            }
        }
        anyhow::ensure!(boundaries.is_empty(), "file storage is missed");

        let write = Self {
            files,
            id2file,
            next_file: 0,
        };

        let read_sync_init = StorageFilesSyncInit {
            files_paths,
            id2file: write.id2file.clone(),
        };

        Ok((write, read_sync_init))
    }

    async fn open_file(config: ConfigStorageFile) -> anyhow::Result<StorageFile> {
        let (file, file_size) = util::open(&config.path).await?;

        // verify file size
        if file_size == 0 {
            file.fallocate(0, config.size, libc::FALLOC_FL_ZERO_RANGE)
                .await
                .with_context(|| format!("failed to preallocate {:?}", config.path))?;
        } else if config.size != file_size {
            anyhow::bail!(
                "invalid file size {:?}: {file_size} (expected: {})",
                config.path,
                config.size
            );
        }

        Ok(StorageFile {
            id: config.id,
            file,
            tail: 0,
            head: 0,
            size: config.size,
        })
    }

    pub async fn close(self) {
        join_all(self.files.into_iter().map(|file| async move {
            let _: io::Result<()> = file.file.close().await;
        }))
        .await;
    }

    pub async fn push_block(
        &mut self,
        slot: Slot,
        block: Option<ConfirmedBlockWithBinary>,
        blocks: &mut StoredBlocksWrite,
    ) -> anyhow::Result<()> {
        if blocks.is_full() {
            self.pop_block(blocks).await?;
        }

        let Some(block) = block else {
            blocks.push_block_dead(slot).await?;
            return Ok(());
        };
        let buffer = block.get_protobuf();
        let buffer_size = buffer.len() as u64;

        let file_index = loop {
            match self.get_file_index_for_new_block(buffer_size) {
                Some(index) => break index,
                None => self.pop_block(blocks).await?,
            }
        };

        let file = &mut self.files[file_index];
        let (offset, _buffer) = file
            .write(buffer)
            .await
            .with_context(|| format!("failed to write block to file id#{}", file.id))?;

        blocks
            .push_block_confirmed(slot, block.block_time, file.id, offset, buffer_size)
            .await?;

        Ok(())
    }

    async fn pop_block(&mut self, blocks: &mut StoredBlocksWrite) -> anyhow::Result<()> {
        let Some(block) = blocks.pop_block().await? else {
            anyhow::bail!("no blocks to remove");
        };

        if block.size == 0 {
            return Ok(());
        }

        let Some(file_index) = self.id2file.get(&block.storage_id).copied() else {
            anyhow::bail!("unknown storage id: {}", block.storage_id);
        };
        let file = &mut self.files[file_index];

        file.tail = block.size + block.offset;
        anyhow::ensure!(
            file.tail <= file.size,
            "file storage tail overflow, {} vs {}",
            file.tail,
            file.size
        );

        Ok(())
    }

    fn get_file_index_for_new_block(&mut self, size: u64) -> Option<usize> {
        let current_index = self.next_file;
        loop {
            let index = self.next_file;
            self.next_file = (self.next_file + 1) % self.files.len();

            if self.files[index].free_space() >= size {
                return Some(index);
            }

            if self.next_file == current_index {
                return None;
            }
        }
    }
}

#[derive(Debug)]
struct StorageFile {
    id: StorageId,
    file: File,
    tail: u64,
    head: u64,
    size: u64,
}

impl StorageFile {
    fn free_space(&self) -> u64 {
        if self.head < self.tail {
            self.tail - self.head
        } else {
            self.tail.max(self.size - self.head)
        }
    }

    async fn write(&mut self, buffer: Vec<u8>) -> anyhow::Result<(u64, Vec<u8>)> {
        let len = buffer.len() as u64;
        anyhow::ensure!(self.free_space() >= len, "not enough space");

        // update head if not enough space
        if self.head > self.tail && self.size - self.head < len {
            self.head = 0;
        }

        let (result, buffer) = self.file.write_all_at(buffer, self.head).await;
        let () = result?;
        self.file.sync_data().await?;

        let offset = self.head;
        self.head += len;
        anyhow::ensure!(
            self.head <= self.size,
            "file storage head overflow, {} vs {}",
            self.head,
            self.size
        );

        Ok((offset, buffer))
    }
}

#[derive(Debug, Clone)]
pub struct StorageFilesSyncInit {
    files_paths: Vec<PathBuf>,
    id2file: HashMap<StorageId, usize>,
}
