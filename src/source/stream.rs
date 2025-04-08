use {
    crate::{
        config::{ConfigSourceStream, ConfigSourceStreamKind},
        source::{block::BlockWithBinary, transaction::TransactionWithBinary},
    },
    futures::{StreamExt, ready, stream::Stream},
    maplit::hashmap,
    richat_client::{grpc::GrpcClientBuilderError, stream::SubscribeStream},
    richat_proto::{
        convert_from::{create_reward, create_tx_with_meta},
        geyser::{
            CommitmentLevel as CommitmentLevelProto, SlotStatus as SlotStatusProto,
            SubscribeRequest, SubscribeRequestFilterBlocksMeta, SubscribeRequestFilterSlots,
            SubscribeRequestFilterTransactions, SubscribeUpdate, SubscribeUpdateBlockMeta,
            SubscribeUpdateSlot, SubscribeUpdateTransaction, subscribe_update::UpdateOneof,
        },
        richat::{GrpcSubscribeRequest, RichatFilter},
    },
    solana_sdk::clock::Slot,
    solana_transaction_status::TransactionWithStatusMeta,
    std::{
        collections::BTreeMap,
        pin::Pin,
        task::{Context, Poll},
    },
    thiserror::Error,
    tracing::{info, warn},
};

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error(transparent)]
    Build(#[from] GrpcClientBuilderError),
    #[error(transparent)]
    Connect(#[from] tonic::Status),
}

#[derive(Debug, Error)]
pub enum RecvError {
    #[error(transparent)]
    Recv(#[from] richat_client::error::ReceiveError),
    #[error("missed field: {0}")]
    MissedField(&'static str),
    #[error("unexpected message: {0}")]
    UnexpectedMessage(&'static str),
    #[error("unknown commitment level: {0}")]
    UnknownCommitmentLevel(i32),
    #[error("parent slot is missed for: {0}")]
    MissedParent(Slot),
    #[error("unexpected commitment level, {0:?} -> {1:?}")]
    UnexpectedCommitment(SlotStatusProto, SlotStatusProto),
    #[error("failed to decode Transaction: {0}")]
    TransactionWithMetaFailed(&'static str),
    #[error("failed to build reward: {0}")]
    RewardsFailed(&'static str),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StreamSourceSlotStatus {
    Dead,
    Confirmed,
    Finalized,
}

#[derive(Debug)]
pub enum StreamSourceMessage {
    Block {
        slot: Slot,
        block: BlockWithBinary,
    },
    SlotStatus {
        slot: Slot,
        parent: Slot,
        status: StreamSourceSlotStatus,
    },
    SlotStatusInit {
        slot: Slot,
        status: StreamSourceSlotStatus,
    },
}

#[derive(Debug)]
struct SlotInfo {
    slot: Slot,
    status: SlotStatusProto,
    parent: Option<Slot>,
    transactions: Vec<(u64, TransactionWithBinary)>,
    block_meta: Option<SubscribeUpdateBlockMeta>,
    sealed: bool,
    ignore_block_build_fail: bool,
}

impl Drop for SlotInfo {
    fn drop(&mut self) {
        if !self.sealed && !self.ignore_block_build_fail {
            warn!(
                slot = self.slot,
                status = ?self.status,
                parent = self.parent,
                transactions = self.transactions.len(),
                executed_transaction_count = self
                    .block_meta
                    .as_ref()
                    .map(|bm| bm.executed_transaction_count),
                "block build failed"
            );
        }
    }
}

impl SlotInfo {
    fn new(slot: Slot) -> Self {
        Self {
            slot,
            status: SlotStatusProto::SlotCreatedBank,
            parent: None,
            transactions: Vec::with_capacity(8_192),
            block_meta: None,
            sealed: false,
            ignore_block_build_fail: false,
        }
    }

    fn try_build_block(&mut self) -> Option<Result<BlockWithBinary, RecvError>> {
        if self.sealed
            || self
                .block_meta
                .as_ref()
                .map(|bm| bm.executed_transaction_count)
                != Some(self.transactions.len() as u64)
        {
            return None;
        }
        self.sealed = true;

        let block_meta = self.block_meta.take()?;
        let (rewards, num_partitions) = match block_meta.rewards {
            Some(obj) => {
                match obj
                    .rewards
                    .into_iter()
                    .map(create_reward)
                    .collect::<Result<Vec<_>, _>>()
                {
                    Ok(rewards) => (rewards, obj.num_partitions.map(|obj| obj.num_partitions)),
                    Err(error) => return Some(Err(RecvError::RewardsFailed(error))),
                }
            }
            None => (vec![], None),
        };

        let mut transactions = std::mem::take(&mut self.transactions);
        transactions.sort_unstable_by_key(|(index, _tx)| *index);

        Some(Ok(BlockWithBinary::new(
            block_meta.parent_blockhash,
            block_meta.blockhash,
            block_meta.parent_slot,
            transactions.into_iter().map(|(_index, tx)| tx).collect(),
            rewards,
            num_partitions,
            block_meta.block_time.map(|obj| obj.timestamp),
            block_meta.block_height.map(|obj| obj.block_height),
        )))
    }
}

#[derive(Debug)]
pub struct StreamSource {
    stream: SubscribeStream,
    slots: BTreeMap<Slot, SlotInfo>,
    first_processed: Option<Slot>,
}

impl StreamSource {
    pub async fn new(config: ConfigSourceStream) -> Result<Self, ConnectError> {
        let mut connection = config.config.connect().await?;

        let version = connection.get_version().await?;
        info!(version = version.version, "connected to Stream");

        let stream = match config.source {
            ConfigSourceStreamKind::DragonsMouth => connection
                .subscribe_dragons_mouth_once(Self::create_dragons_mouth_filter())
                .await?
                .into_parsed(),
            ConfigSourceStreamKind::Richat => connection
                .subscribe_richat(GrpcSubscribeRequest {
                    replay_from_slot: None,
                    filter: Self::create_richat_filter(),
                })
                .await?
                .into_parsed(),
        };

        Ok(Self {
            stream,
            slots: BTreeMap::new(),
            first_processed: None,
        })
    }

    const fn create_richat_filter() -> Option<RichatFilter> {
        Some(RichatFilter {
            disable_accounts: true,
            disable_transactions: false,
            disable_entries: true,
        })
    }

    fn create_dragons_mouth_filter() -> SubscribeRequest {
        SubscribeRequest {
            accounts: hashmap! {},
            slots: hashmap! { "".to_owned() => SubscribeRequestFilterSlots {
                filter_by_commitment: Some(false),
                interslot_updates: Some(true),
            } },
            transactions: hashmap! { "".to_owned() => SubscribeRequestFilterTransactions::default() },
            transactions_status: hashmap! {},
            blocks: hashmap! {},
            blocks_meta: hashmap! { "".to_owned() => SubscribeRequestFilterBlocksMeta::default() },
            entry: hashmap! {},
            commitment: Some(CommitmentLevelProto::Processed as i32),
            accounts_data_slice: vec![],
            ping: None,
            from_slot: None,
        }
    }
}

impl Stream for StreamSource {
    type Item = Result<StreamSourceMessage, RecvError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.as_mut();
        loop {
            return Poll::Ready(Some(match ready!(this.stream.poll_next_unpin(cx)) {
                Some(Ok(SubscribeUpdate { update_oneof, .. })) => match update_oneof {
                    Some(UpdateOneof::Account(_)) => Err(RecvError::UnexpectedMessage("Account")),
                    Some(UpdateOneof::Slot(SubscribeUpdateSlot {
                        slot,
                        parent,
                        status,
                        ..
                    })) => {
                        let status = match SlotStatusProto::try_from(status) {
                            Ok(SlotStatusProto::SlotProcessed) => continue,
                            Ok(SlotStatusProto::SlotConfirmed) => SlotStatusProto::SlotConfirmed,
                            Ok(SlotStatusProto::SlotFinalized) => SlotStatusProto::SlotFinalized,
                            Ok(SlotStatusProto::SlotFirstShredReceived) => continue,
                            Ok(SlotStatusProto::SlotCompleted) => continue,
                            Ok(SlotStatusProto::SlotCreatedBank) => {
                                SlotStatusProto::SlotCreatedBank
                            }
                            Ok(SlotStatusProto::SlotDead) => SlotStatusProto::SlotDead,
                            Err(_error) => {
                                return Poll::Ready(Some(Err(RecvError::UnknownCommitmentLevel(
                                    status,
                                ))));
                            }
                        };

                        // store first processed slot
                        if status == SlotStatusProto::SlotCreatedBank
                            && this.first_processed.is_none()
                        {
                            this.first_processed = Some(slot);
                        }

                        // drop message if less or eq to first processed
                        let first_processed = match this.first_processed {
                            Some(first_processed) if slot > first_processed => first_processed,
                            _ if matches!(
                                status,
                                SlotStatusProto::SlotConfirmed | SlotStatusProto::SlotFinalized
                            ) =>
                            {
                                return Poll::Ready(Some(Ok(
                                    StreamSourceMessage::SlotStatusInit {
                                        slot,
                                        status: match status {
                                            SlotStatusProto::SlotConfirmed => {
                                                StreamSourceSlotStatus::Confirmed
                                            }
                                            SlotStatusProto::SlotFinalized => {
                                                StreamSourceSlotStatus::Finalized
                                            }
                                            _ => unreachable!(),
                                        },
                                    },
                                )));
                            }
                            _ => continue,
                        };

                        // drop outdated slots
                        if status == SlotStatusProto::SlotFinalized {
                            loop {
                                match this.slots.keys().next().copied() {
                                    Some(slot_first) if slot_first < slot => {
                                        if let Some(mut slot_info) = this.slots.remove(&slot_first)
                                        {
                                            // ignore warn message if we expect that block would not be complete
                                            if slot_first <= first_processed {
                                                slot_info.ignore_block_build_fail = true;
                                            }
                                            drop(slot_info);
                                        }
                                    }
                                    _ => break,
                                }
                            }
                        }

                        // create slot info
                        let entry = this.slots.entry(slot);
                        let slot_info = entry.or_insert_with(|| SlotInfo::new(slot));

                        // store parent and drop message (only processed)
                        if status == SlotStatusProto::SlotCreatedBank {
                            if slot_info.parent.is_none()
                                && slot_info.status == SlotStatusProto::SlotCreatedBank
                            {
                                if let Some(parent) = parent {
                                    slot_info.parent = Some(parent);
                                    continue;
                                }
                            }

                            if first_processed <= slot {
                                return Poll::Ready(Some(Err(RecvError::UnexpectedCommitment(
                                    slot_info.status,
                                    status,
                                ))));
                            }
                            continue;
                        }

                        // update status
                        let parent = match (slot_info.parent, slot_info.status, status) {
                            (
                                Some(parent),
                                SlotStatusProto::SlotCreatedBank,
                                SlotStatusProto::SlotDead,
                            ) => {
                                slot_info.status = SlotStatusProto::SlotDead;
                                parent
                            }
                            (
                                Some(parent),
                                SlotStatusProto::SlotCreatedBank,
                                SlotStatusProto::SlotConfirmed,
                            ) => {
                                slot_info.status = SlotStatusProto::SlotConfirmed;
                                parent
                            }
                            (
                                Some(parent),
                                SlotStatusProto::SlotConfirmed,
                                SlotStatusProto::SlotFinalized,
                            ) => {
                                slot_info.status = SlotStatusProto::SlotFinalized;
                                parent
                            }
                            _ => {
                                return Poll::Ready(Some(Err(RecvError::UnexpectedCommitment(
                                    slot_info.status,
                                    status,
                                ))));
                            }
                        };

                        Ok(StreamSourceMessage::SlotStatus {
                            slot,
                            parent,
                            status: match status {
                                SlotStatusProto::SlotDead => StreamSourceSlotStatus::Dead,
                                SlotStatusProto::SlotConfirmed => StreamSourceSlotStatus::Confirmed,
                                SlotStatusProto::SlotFinalized => StreamSourceSlotStatus::Finalized,
                                _ => unreachable!(),
                            },
                        })
                    }
                    Some(UpdateOneof::Transaction(SubscribeUpdateTransaction {
                        transaction,
                        slot,
                    })) => match transaction {
                        Some(tx) => {
                            let first_processed = this.first_processed;
                            let entry = this.slots.entry(slot);
                            let slot_info = entry.or_insert_with(|| SlotInfo::new(slot));
                            let is_vote = tx.is_vote;
                            let index = tx.index;
                            match create_tx_with_meta(tx) {
                                Ok(tx) => {
                                    if let TransactionWithStatusMeta::MissingMetadata(tx) = &tx {
                                        warn!(slot, signature = ?tx.signatures[0], "missing metadata");
                                    }

                                    slot_info.transactions.push((
                                        index,
                                        TransactionWithBinary::new(slot, tx, Some(is_vote)),
                                    ));
                                    if let Some(first_processed) = first_processed {
                                        if slot <= first_processed {
                                            continue;
                                        }
                                    }
                                    match slot_info.try_build_block() {
                                        Some(Ok(block)) => {
                                            Ok(StreamSourceMessage::Block { slot, block })
                                        }
                                        Some(Err(error)) => Err(error),
                                        None => continue,
                                    }
                                }
                                Err(error) => Err(RecvError::TransactionWithMetaFailed(error)),
                            }
                        }
                        None => Err(RecvError::MissedField("transaction")),
                    },
                    Some(UpdateOneof::TransactionStatus(_)) => {
                        Err(RecvError::UnexpectedMessage("TransactionStatus"))
                    }
                    Some(UpdateOneof::Block(_)) => Err(RecvError::UnexpectedMessage("Block")),
                    Some(UpdateOneof::Ping(_)) => continue,
                    Some(UpdateOneof::Pong(_)) => Err(RecvError::UnexpectedMessage("Pong")),
                    Some(UpdateOneof::BlockMeta(block_meta)) => {
                        let slot = block_meta.slot;
                        let first_processed = this.first_processed;
                        let entry = this.slots.entry(slot);
                        let slot_info = entry.or_insert_with(|| SlotInfo::new(slot));
                        slot_info.block_meta = Some(block_meta);
                        if let Some(first_processed) = first_processed {
                            if slot <= first_processed {
                                continue;
                            }
                        }
                        match slot_info.try_build_block() {
                            Some(Ok(block)) => Ok(StreamSourceMessage::Block { slot, block }),
                            Some(Err(error)) => Err(error),
                            None => continue,
                        }
                    }
                    Some(UpdateOneof::Entry(_)) => Err(RecvError::UnexpectedMessage("Entry")),
                    None => Err(RecvError::MissedField("update_oneof")),
                },
                Some(Err(error)) => Err(error.into()),
                None => return Poll::Ready(None),
            }));
        }
    }
}
