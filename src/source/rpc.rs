use {
    crate::{
        config::ConfigSourceRpc,
        source::{block::BlockWithBinary, transaction::TransactionWithBinary},
    },
    base64::{Engine, prelude::BASE64_STANDARD},
    solana_client::{
        client_error::{ClientError, ClientErrorKind},
        nonblocking::rpc_client::RpcClient,
        rpc_client::RpcClientConfig,
        rpc_config::RpcBlockConfig,
        rpc_custom_error::{
            JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE, JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
        },
        rpc_request::RpcError,
    },
    solana_rpc_client::http_sender::HttpSender,
    solana_sdk::{
        clock::Slot,
        commitment_config::CommitmentConfig,
        instruction::CompiledInstruction,
        message::{Message, VersionedMessage, v0::LoadedAddresses},
        transaction::Transaction,
        transaction_context::TransactionReturnData,
    },
    solana_transaction_status::{
        ConfirmedBlock, EncodedTransactionWithStatusMeta, InnerInstruction, InnerInstructions,
        TransactionDetails, TransactionStatusMeta, TransactionTokenBalance,
        TransactionWithStatusMeta, UiCompiledInstruction, UiConfirmedBlock, UiInnerInstructions,
        UiInstruction, UiLoadedAddresses, UiReturnDataEncoding, UiTransactionEncoding,
        UiTransactionReturnData, UiTransactionStatusMeta, UiTransactionTokenBalance,
        VersionedTransactionWithStatusMeta, option_serializer::OptionSerializer,
    },
    std::fmt,
    thiserror::Error,
    tokio::sync::Semaphore,
    tracing::{info, warn},
};

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error(transparent)]
    Client(#[from] ClientError),
}

#[derive(Debug, Error)]
pub enum GetBlockError {
    #[error(transparent)]
    Request(#[from] ClientError),
    #[error("Slot {0} was skipped, or missing due to ledger jump to recent snapshot")]
    SlotSkipped(Slot),
    #[error("Block not available for slot {0}")]
    BlockNotAvailable(Slot),
    #[error(transparent)]
    Decode(#[from] BlockDecodeError),
}

#[derive(Debug, Error, Clone, Copy, PartialEq, Eq)]
pub enum BlockDecodeError {
    #[error("UiConfirmedBlock::transactions is missed")]
    MissedTransactions,
    #[error("UiConfirmedBlock::rewards is missed")]
    MissedRewards,
    #[error("VersionedTransaction failed to create")]
    FailedVersionedTransaction,
    #[error("UiTransactionStatusMeta::loaded_addresses is missed")]
    MissedLoadedAddresses,
    #[error("TransactionStatusMeta::inner_instructions failed to create")]
    FailedInnerInstructions,
    #[error("LoadedAddresses::writable failed to create")]
    FailedLoadedAddressesWritable,
    #[error("LoadedAddresses::readonly failed to create")]
    FailedLoadedAddressesReadonly,
    #[error("TransactionReturnData::program_id failed to create")]
    FailedTransactionReturnDataProgramId,
    #[error("TransactionReturnData::data failed to create")]
    FailedTransactionReturnData,
}

pub struct RpcSource {
    client: RpcClient,
    semaphore: Semaphore,
}

impl fmt::Debug for RpcSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RpcSource").finish()
    }
}

impl RpcSource {
    pub async fn new(config: ConfigSourceRpc) -> Result<Self, ConnectError> {
        let sender = HttpSender::new_with_timeout(config.url, config.timeout);
        let client = RpcClient::new_sender(sender, RpcClientConfig::default());

        let version = client.get_version().await?;
        info!(version = version.solana_core, "connected to RPC");

        Ok(Self {
            client,
            semaphore: Semaphore::new(config.concurrency),
        })
    }

    pub async fn get_confirmed_slot(&self) -> Result<Slot, ClientError> {
        let _permit = self.semaphore.acquire().await.expect("unclosed");
        self.client
            .get_slot_with_commitment(CommitmentConfig::confirmed())
            .await
    }

    pub async fn get_block(&self, slot: Slot) -> Result<BlockWithBinary, GetBlockError> {
        let config = RpcBlockConfig {
            encoding: Some(UiTransactionEncoding::Base64),
            transaction_details: Some(TransactionDetails::Full),
            rewards: Some(true),
            commitment: Some(CommitmentConfig::confirmed()),
            max_supported_transaction_version: Some(u8::MAX),
        };

        let permit = self.semaphore.acquire().await.expect("unclosed");
        let response = self.client.get_block_with_config(slot, config).await;
        drop(permit);

        let block = match response {
            Ok(block) => block,
            // not confirmed yet?
            Err(ClientError {
                kind:
                    ClientErrorKind::RpcError(RpcError::RpcResponseError {
                        code: JSON_RPC_SERVER_ERROR_BLOCK_NOT_AVAILABLE,
                        ..
                    }),
                ..
            }) => {
                return Err(GetBlockError::BlockNotAvailable(slot));
            }
            // dead
            Err(ClientError {
                kind:
                    ClientErrorKind::RpcError(RpcError::RpcResponseError {
                        code: JSON_RPC_SERVER_ERROR_SLOT_SKIPPED,
                        ..
                    }),
                ..
            }) => {
                return Err(GetBlockError::SlotSkipped(slot));
            }
            Err(error) => {
                return Err(error.into());
            }
        };

        let block = Self::block_decode(block)?;
        for tx in &block.transactions {
            if let TransactionWithStatusMeta::MissingMetadata(tx) = &tx {
                warn!(slot, signature = ?tx.signatures[0], "missing metadata");
            }
        }

        Ok(BlockWithBinary::new(
            block.previous_blockhash,
            block.blockhash,
            block.parent_slot,
            block
                .transactions
                .into_iter()
                .map(TransactionWithBinary::new)
                .collect(),
            block.rewards,
            block.num_partitions,
            block.block_time,
            block.block_height,
        ))
    }

    fn block_decode(block: UiConfirmedBlock) -> Result<ConfirmedBlock, BlockDecodeError> {
        Ok(ConfirmedBlock {
            previous_blockhash: block.previous_blockhash,
            blockhash: block.blockhash,
            parent_slot: block.parent_slot,
            transactions: block
                .transactions
                .ok_or(BlockDecodeError::MissedTransactions)?
                .into_iter()
                .map(Self::tx_decode)
                .collect::<Result<Vec<_>, _>>()?,
            rewards: block.rewards.ok_or(BlockDecodeError::MissedRewards)?,
            num_partitions: block.num_reward_partitions,
            block_time: block.block_time,
            block_height: block.block_height,
        })
    }

    fn tx_decode(
        tx: EncodedTransactionWithStatusMeta,
    ) -> Result<TransactionWithStatusMeta, BlockDecodeError> {
        let EncodedTransactionWithStatusMeta {
            transaction,
            meta,
            version: _version,
        } = tx;

        let transaction = transaction
            .decode()
            .ok_or(BlockDecodeError::FailedVersionedTransaction)?;

        match meta {
            Some(meta) => Ok(TransactionWithStatusMeta::Complete(
                VersionedTransactionWithStatusMeta {
                    transaction,
                    meta: Self::tx_meta_decode(meta)?,
                },
            )),
            None => Ok(TransactionWithStatusMeta::MissingMetadata(Transaction {
                signatures: transaction.signatures,
                message: match transaction.message {
                    VersionedMessage::Legacy(message) => message,
                    VersionedMessage::V0(v0) => Message {
                        header: v0.header,
                        account_keys: v0.account_keys,
                        recent_blockhash: v0.recent_blockhash,
                        instructions: v0.instructions,
                    },
                },
            })),
        }
    }

    fn tx_meta_decode(
        meta: UiTransactionStatusMeta,
    ) -> Result<TransactionStatusMeta, BlockDecodeError> {
        let (la_writable, la_readonly) = match meta.loaded_addresses.into() {
            Some(UiLoadedAddresses { writable, readonly }) => (writable, readonly),
            None => return Err(BlockDecodeError::MissedLoadedAddresses),
        };

        Ok(TransactionStatusMeta {
            status: meta.status,
            fee: meta.fee,
            pre_balances: meta.pre_balances,
            post_balances: meta.post_balances,
            inner_instructions: Option::<Vec<UiInnerInstructions>>::from(meta.inner_instructions)
                .map(Self::tx_meta_inner_instructions_conv)
                .transpose()?,
            log_messages: meta.log_messages.into(),
            pre_token_balances: Option::<Vec<UiTransactionTokenBalance>>::from(
                meta.pre_token_balances,
            )
            .map(Self::tx_meta_token_balances_conv),
            post_token_balances: Option::<Vec<UiTransactionTokenBalance>>::from(
                meta.post_token_balances,
            )
            .map(Self::tx_meta_token_balances_conv),
            rewards: meta.rewards.into(),
            loaded_addresses: LoadedAddresses {
                writable: la_writable
                    .iter()
                    .map(|pk| pk.parse().ok())
                    .collect::<Option<Vec<_>>>()
                    .ok_or(BlockDecodeError::FailedLoadedAddressesWritable)?,
                readonly: la_readonly
                    .iter()
                    .map(|pk| pk.parse().ok())
                    .collect::<Option<Vec<_>>>()
                    .ok_or(BlockDecodeError::FailedLoadedAddressesReadonly)?,
            },
            return_data: Option::<UiTransactionReturnData>::from(meta.return_data)
                .map(Self::tx_meta_return_data_conv)
                .transpose()?,
            compute_units_consumed: meta.compute_units_consumed.into(),
        })
    }

    fn tx_meta_inner_instructions_conv(
        ixs: Vec<UiInnerInstructions>,
    ) -> Result<Vec<InnerInstructions>, BlockDecodeError> {
        ixs.into_iter()
            .map(|ix| {
                ix.instructions
                    .into_iter()
                    .map(|ui_ix| match ui_ix {
                        UiInstruction::Compiled(ix) => Some(Self::ix_compiled_decode(ix)),
                        UiInstruction::Parsed(_) => None,
                    })
                    .collect::<Option<Vec<_>>>()
                    .map(|instructions| InnerInstructions {
                        index: ix.index,
                        instructions,
                    })
            })
            .collect::<Option<Vec<_>>>()
            .ok_or(BlockDecodeError::FailedInnerInstructions)
    }

    fn ix_compiled_decode(ix: UiCompiledInstruction) -> InnerInstruction {
        InnerInstruction {
            instruction: CompiledInstruction {
                accounts: ix.accounts,
                program_id_index: ix.program_id_index,
                data: bs58::decode(ix.data).into_vec().unwrap(),
            },
            stack_height: ix.stack_height,
        }
    }

    fn tx_meta_token_balances_conv(
        token_balances: Vec<UiTransactionTokenBalance>,
    ) -> Vec<TransactionTokenBalance> {
        token_balances
            .into_iter()
            .map(|balance| TransactionTokenBalance {
                account_index: balance.account_index,
                mint: balance.mint,
                ui_token_amount: balance.ui_token_amount,
                owner: match balance.owner {
                    OptionSerializer::Some(value) => value,
                    _ => Default::default(),
                },
                program_id: match balance.program_id {
                    OptionSerializer::Some(value) => value,
                    _ => Default::default(),
                },
            })
            .collect::<Vec<_>>()
    }

    fn tx_meta_return_data_conv(
        return_data: UiTransactionReturnData,
    ) -> Result<TransactionReturnData, BlockDecodeError> {
        Ok(TransactionReturnData {
            program_id: return_data
                .program_id
                .parse()
                .map_err(|_error| BlockDecodeError::FailedTransactionReturnDataProgramId)?,
            data: match return_data.data.1 {
                UiReturnDataEncoding::Base64 => BASE64_STANDARD
                    .decode(return_data.data.0)
                    .map_err(|_error| BlockDecodeError::FailedTransactionReturnData)?,
            },
        })
    }
}
