use {
    foldhash::quality::SeedableRandomState, prost::Message as _, solana_sdk::signature::Signature,
    solana_storage_proto::convert::generated, solana_transaction_status::TransactionWithStatusMeta,
    std::hash::BuildHasher,
};

#[derive(Debug)]
pub struct TransactionWithBinary {
    pub hash: u64,
    pub signature: Signature,
    pub protobuf: Vec<u8>,
}

impl TransactionWithBinary {
    pub fn new(tx: TransactionWithStatusMeta) -> Self {
        thread_local! {
            static HASHER: SeedableRandomState = SeedableRandomState::fixed();
        }

        let signature = *tx.transaction_signature();
        let hash = HASHER.with(|hasher| hasher.hash_one(signature));
        let protobuf = generated::ConfirmedTransaction::from(tx).encode_to_vec();

        Self {
            hash,
            signature,
            protobuf,
        }
    }
}
