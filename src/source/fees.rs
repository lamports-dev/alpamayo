use {
    crate::{
        source::transaction::TransactionWithBinary,
        util::{HashMap, HashSet},
    },
    solana_sdk::pubkey::Pubkey,
    solana_transaction_status::{
        TransactionWithStatusMeta, VersionedTransactionWithStatusMeta, parse_vote::parse_vote,
    },
    tracing::warn,
};

#[derive(Debug)]
pub struct TransactionFees {
    unit_price: u64,
    account_keys: HashSet<Pubkey>,
}

impl TransactionFees {
    pub fn create(tx: &TransactionWithStatusMeta, is_vote: Option<bool>) -> Option<Self> {
        let TransactionWithStatusMeta::Complete(tx) = tx else {
            return None;
        };

        if is_vote == Some(true) || Self::is_vote(tx) {
            return None;
        }

        let account_keys = tx.account_keys();
        let Some(instructions) = tx
            .transaction
            .message
            .instructions()
            .iter()
            .map(|ix| {
                account_keys
                    .get(ix.program_id_index as usize)
                    .map(|program_id| (program_id, ix))
            })
            .collect::<Option<Vec<_>>>()
        else {
            warn!("failed to get program id");
            return None;
        };

        let computed_budget_limits =
            match compute_budget_processor::process_compute_budget_instructions(
                instructions.into_iter(),
            ) {
                Ok(value) => value,
                Err(error) => {
                    warn!(?error, "failed to compute transaction budget");
                    return None;
                }
            };

        // count only transactions with limit
        if computed_budget_limits.compute_unit_limit == 0 {
            return None;
        }

        let mut account_keys = HashSet::default();
        for (index, pubkey) in tx
            .transaction
            .message
            .static_account_keys()
            .iter()
            .enumerate()
        {
            if tx.transaction.message.is_maybe_writable(index, None) {
                account_keys.insert(*pubkey);
            }
        }

        Some(Self {
            unit_price: computed_budget_limits.compute_unit_price,
            account_keys,
        })
    }

    fn is_vote(tx: &VersionedTransactionWithStatusMeta) -> bool {
        let account_keys = tx.account_keys();
        for instruction in tx.transaction.message.instructions() {
            if parse_vote(instruction, &account_keys).is_ok() {
                return true;
            }
        }
        false
    }
}

#[derive(Debug)]
pub struct TransactionsFees {
    fees: Vec<u64>,
    writable_account_fees: HashMap<Pubkey, Vec<u64>>,
}

impl TransactionsFees {
    pub fn new(txs: &[TransactionWithBinary]) -> Self {
        let mut fees = Vec::with_capacity(txs.len());
        let mut writable_account_fees =
            HashMap::<Pubkey, Vec<u64>>::with_capacity_and_hasher(txs.len(), Default::default());

        for txfee in txs.iter().filter_map(|tx| tx.fees.as_ref()) {
            fees.push(txfee.unit_price);
            for account in txfee.account_keys.iter() {
                writable_account_fees
                    .entry(*account)
                    .or_default()
                    .push(txfee.unit_price);
            }
        }

        fees.sort_unstable();
        for value in writable_account_fees.values_mut() {
            value.sort_unstable();
        }

        Self {
            fees,
            writable_account_fees,
        }
    }

    pub fn get_fee(&self, accounts: &[Pubkey], percentile: Option<u16>) -> u64 {
        let mut fee = Self::get_with_percentile(&self.fees, percentile);

        if let Some(afee) = accounts
            .iter()
            .filter_map(|account| {
                self.writable_account_fees
                    .get(account)
                    .map(|fees| Self::get_with_percentile(fees, percentile))
            })
            .reduce(|fee1, fee2| fee1.max(fee2))
        {
            fee = fee.max(afee);
        }

        fee
    }

    fn get_with_percentile(fees: &[u64], percentile: Option<u16>) -> u64 {
        let fee = match percentile {
            Some(percentile) => Self::get_percentile(fees, percentile),
            None => fees.first().copied(),
        };
        fee.unwrap_or_default()
    }

    fn get_percentile(fees: &[u64], percentile: u16) -> Option<u64> {
        let index = (percentile as usize).min(9_999) * fees.len() / 10_000;
        fees.get(index).copied()
    }
}

// Copied from the 2.0 crate
// https://docs.rs/solana-compute-budget/2.0.25/src/solana_compute_budget/compute_budget_processor.rs.html#69-148
mod compute_budget_processor {
    use {
        solana_compute_budget::compute_budget_limits::{
            DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT, MAX_COMPUTE_UNIT_LIMIT, MAX_HEAP_FRAME_BYTES,
            MIN_HEAP_FRAME_BYTES,
        },
        solana_compute_budget_interface::ComputeBudgetInstruction,
        solana_sdk::{
            borsh1::try_from_slice_unchecked,
            instruction::{CompiledInstruction, InstructionError},
            pubkey::Pubkey,
            transaction::TransactionError,
        },
    };

    #[derive(Debug)]
    pub struct ComputeBudgetLimits {
        pub compute_unit_limit: u32,
        pub compute_unit_price: u64,
    }

    pub fn process_compute_budget_instructions<'a>(
        instructions: impl Iterator<Item = (&'a Pubkey, &'a CompiledInstruction)>,
    ) -> Result<ComputeBudgetLimits, TransactionError> {
        let mut num_non_compute_budget_instructions: u32 = 0;
        let mut updated_compute_unit_limit = None;
        let mut updated_compute_unit_price = None;
        let mut requested_heap_size = None;
        let mut updated_loaded_accounts_data_size_limit = None;

        for (i, (program_id, instruction)) in instructions.enumerate() {
            if solana_compute_budget_interface::check_id(program_id) {
                let invalid_instruction_data_error = TransactionError::InstructionError(
                    i as u8,
                    InstructionError::InvalidInstructionData,
                );
                let duplicate_instruction_error = TransactionError::DuplicateInstruction(i as u8);

                match try_from_slice_unchecked(&instruction.data) {
                    Ok(ComputeBudgetInstruction::RequestHeapFrame(bytes)) => {
                        if requested_heap_size.is_some() {
                            return Err(duplicate_instruction_error);
                        }
                        if sanitize_requested_heap_size(bytes) {
                            requested_heap_size = Some(bytes);
                        } else {
                            return Err(invalid_instruction_data_error);
                        }
                    }
                    Ok(ComputeBudgetInstruction::SetComputeUnitLimit(compute_unit_limit)) => {
                        if updated_compute_unit_limit.is_some() {
                            return Err(duplicate_instruction_error);
                        }
                        updated_compute_unit_limit = Some(compute_unit_limit);
                    }
                    Ok(ComputeBudgetInstruction::SetComputeUnitPrice(micro_lamports)) => {
                        if updated_compute_unit_price.is_some() {
                            return Err(duplicate_instruction_error);
                        }
                        updated_compute_unit_price = Some(micro_lamports);
                    }
                    Ok(ComputeBudgetInstruction::SetLoadedAccountsDataSizeLimit(bytes)) => {
                        if updated_loaded_accounts_data_size_limit.is_some() {
                            return Err(duplicate_instruction_error);
                        }
                        updated_loaded_accounts_data_size_limit = Some(bytes);
                    }
                    _ => return Err(invalid_instruction_data_error),
                }
            } else {
                // only include non-request instructions in default max calc
                num_non_compute_budget_instructions =
                    num_non_compute_budget_instructions.saturating_add(1);
            }
        }

        let compute_unit_limit = updated_compute_unit_limit
            .unwrap_or_else(|| {
                num_non_compute_budget_instructions
                    .saturating_mul(DEFAULT_INSTRUCTION_COMPUTE_UNIT_LIMIT)
            })
            .min(MAX_COMPUTE_UNIT_LIMIT);

        let compute_unit_price = updated_compute_unit_price.unwrap_or(0);

        Ok(ComputeBudgetLimits {
            compute_unit_limit,
            compute_unit_price,
        })
    }

    fn sanitize_requested_heap_size(bytes: u32) -> bool {
        (MIN_HEAP_FRAME_BYTES..=MAX_HEAP_FRAME_BYTES).contains(&bytes) && bytes % 1024 == 0
    }
}
