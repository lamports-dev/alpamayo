use {
    crate::storage::{
        blocks::{StorageBlockHeaderLocationResult, StoredBlockHeaders},
        files::StorageFiles,
    },
    futures::{FutureExt, future::LocalBoxFuture},
    solana_sdk::clock::Slot,
    std::{io, time::Instant},
    tokio::{sync::oneshot, time::timeout_at},
    tracing::error,
};

#[derive(Debug)]
pub enum ReadResultGetBlock {
    Timeout,
    Removed,
    Dead,
    NotAvailable,
    Block(Vec<u8>),
    ReadError(io::Error),
}

#[derive(Debug)]
pub enum ReadRequest {
    GetBlock {
        deadline: Instant,
        slot: Slot,
        tx: oneshot::Sender<ReadResultGetBlock>,
    },
}

impl ReadRequest {
    pub fn process<'a>(
        self,
        blocks_files: &StorageFiles,
        blocks_headers: &StoredBlockHeaders,
    ) -> Option<LocalBoxFuture<'a, ()>> {
        match self {
            Self::GetBlock { deadline, slot, tx } => {
                if deadline < Instant::now() {
                    let _ = tx.send(ReadResultGetBlock::Timeout);
                    return None;
                }

                let location = match blocks_headers.get_block_location(slot) {
                    StorageBlockHeaderLocationResult::Removed => {
                        let _ = tx.send(ReadResultGetBlock::Removed);
                        return None;
                    }
                    StorageBlockHeaderLocationResult::Dead => {
                        let _ = tx.send(ReadResultGetBlock::Dead);
                        return None;
                    }
                    StorageBlockHeaderLocationResult::NotAvailable => {
                        let _ = tx.send(ReadResultGetBlock::NotAvailable);
                        return None;
                    }
                    StorageBlockHeaderLocationResult::SlotMismatch => {
                        error!(slot, "item/slot mismatch");
                        let _ = tx.send(ReadResultGetBlock::ReadError(io::Error::new(
                            io::ErrorKind::Other,
                            "item/slot mismatch",
                        )));
                        return None;
                    }
                    StorageBlockHeaderLocationResult::Found(location) => location,
                };

                let read_fut =
                    blocks_files.read(location.storage_id, location.offset, location.size);
                let fut = async move {
                    let ts = Instant::now();
                    let result = match timeout_at(deadline.into(), read_fut).await {
                        Ok(Ok(block)) => ReadResultGetBlock::Block(block),
                        Ok(Err(error)) => ReadResultGetBlock::ReadError(error),
                        Err(_error) => ReadResultGetBlock::Timeout,
                    };
                    tracing::error!(slot, elapsed = ?ts.elapsed(), "read tokio");
                    let _ = tx.send(result);
                }
                .boxed_local();
                Some(fut)
            }
        }
    }
}
