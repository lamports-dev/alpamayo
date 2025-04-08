use {
    crate::{
        source::{fees::TransactionFees, sfa::SignatureForAddress},
        storage::rocksdb::TransactionIndex,
    },
    prost::Message as _,
    solana_sdk::{clock::Slot, signature::Signature, transaction::TransactionError},
    solana_storage_proto::convert::generated,
    solana_transaction_status::{TransactionWithStatusMeta, extract_and_fmt_memos},
};

#[derive(Debug)]
pub struct TransactionWithBinary {
    pub key: [u8; 8],
    pub signature: Signature,
    pub err: Option<TransactionError>,
    pub sfa: Vec<SignatureForAddress>,
    pub fees: Option<TransactionFees>,
    pub protobuf: Vec<u8>,
}

impl TransactionWithBinary {
    pub fn new(slot: Slot, tx: TransactionWithStatusMeta, is_vote: Option<bool>) -> Self {
        let signature = *tx.transaction_signature();
        let key = TransactionIndex::encode(&signature);

        let (err, sfa) = match &tx {
            TransactionWithStatusMeta::MissingMetadata(_) => (None, vec![]),
            TransactionWithStatusMeta::Complete(tx) => {
                let account_keys = tx.account_keys();
                let err = tx.meta.status.clone().err();
                let memo = extract_and_fmt_memos(tx);
                let mut sfa = Vec::with_capacity(account_keys.len());
                for pubkey in account_keys.iter() {
                    sfa.push(SignatureForAddress::new(
                        slot,
                        *pubkey,
                        signature,
                        err.clone(),
                        memo.clone(),
                    ))
                }
                (err, sfa)
            }
        };

        let fees = TransactionFees::create(&tx, is_vote);

        let protobuf = generated::ConfirmedTransaction::from(tx).encode_to_vec();

        Self {
            key,
            signature,
            err,
            sfa,
            fees,
            protobuf,
        }
    }
}
