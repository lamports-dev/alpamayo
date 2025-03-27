use {
    crate::storage::rocksdb::TransactionIndex, prost::Message as _,
    solana_sdk::signature::Signature, solana_storage_proto::convert::generated,
    solana_transaction_status::TransactionWithStatusMeta,
};

#[derive(Debug)]
pub struct TransactionWithBinary {
    pub hash: [u8; 8],
    pub signature: Signature,
    pub protobuf: Vec<u8>,
}

impl TransactionWithBinary {
    pub fn new(tx: TransactionWithStatusMeta) -> Self {
        let signature = *tx.transaction_signature();
        let hash = TransactionIndex::key(&signature);
        let protobuf = generated::ConfirmedTransaction::from(tx).encode_to_vec();

        Self {
            hash,
            signature,
            protobuf,
        }
    }
}
