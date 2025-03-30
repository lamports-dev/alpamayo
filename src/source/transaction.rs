use {
    crate::{source::sfa::SignatureForAddress, storage::rocksdb::TransactionIndex},
    prost::Message as _,
    solana_sdk::{clock::Slot, signature::Signature},
    solana_storage_proto::convert::generated,
    solana_transaction_status::{TransactionWithStatusMeta, extract_and_fmt_memos},
};

#[derive(Debug)]
pub struct TransactionWithBinary {
    pub key: [u8; 8],
    pub signature: Signature,
    pub sfa: Vec<SignatureForAddress>,
    pub protobuf: Vec<u8>,
}

impl TransactionWithBinary {
    pub fn new(slot: Slot, tx: TransactionWithStatusMeta) -> Self {
        let signature = *tx.transaction_signature();
        let key = TransactionIndex::encode(&signature);

        let sfa = match &tx {
            TransactionWithStatusMeta::MissingMetadata(_) => vec![],
            TransactionWithStatusMeta::Complete(tx) => {
                let account_keys = tx.account_keys();
                let memo = extract_and_fmt_memos(tx);
                let mut sfa = Vec::with_capacity(account_keys.len());
                for pubkey in account_keys.iter() {
                    sfa.push(SignatureForAddress::new(
                        slot,
                        *pubkey,
                        signature,
                        tx.meta.status.clone().err(),
                        memo.clone(),
                    ))
                }
                sfa
            }
        };

        let protobuf = generated::ConfirmedTransaction::from(tx).encode_to_vec();

        Self {
            key,
            signature,
            sfa,
            protobuf,
        }
    }
}
