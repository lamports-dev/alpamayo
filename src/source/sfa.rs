use {
    crate::storage::rocksdb::SfaIndex,
    solana_sdk::{
        clock::Slot, pubkey::Pubkey, signature::Signature, transaction::TransactionError,
    },
};

#[derive(Debug)]
pub struct SignatureForAddress {
    pub key: [u8; 16],
    pub address_hash: [u8; 8],
    pub address: Pubkey,
    pub signature: Signature,
    pub err: Option<TransactionError>,
    pub memo: Option<String>,
}

impl SignatureForAddress {
    pub fn new(
        slot: Slot,
        address: Pubkey,
        signature: Signature,
        err: Option<TransactionError>,
        memo: Option<String>,
    ) -> Self {
        Self {
            key: SfaIndex::encode(&address, slot),
            address_hash: SfaIndex::address_hash(&address),
            address,
            signature,
            err,
            memo,
        }
    }
}

#[derive(Debug)]
pub struct SignaturesForAddress {
    pub key: [u8; 16],
    pub address_hash: [u8; 8],
    pub signatures: Vec<SignatureStatus>,
}

impl SignaturesForAddress {
    pub fn new(sfa: SignatureForAddress) -> Self {
        Self {
            key: sfa.key,
            address_hash: sfa.address_hash,
            signatures: vec![SignatureStatus {
                signature: sfa.signature,
                err: sfa.err,
                memo: sfa.memo,
            }],
        }
    }

    pub fn merge(&mut self, sfa: SignatureForAddress) {
        self.signatures.push(SignatureStatus {
            signature: sfa.signature,
            err: sfa.err,
            memo: sfa.memo,
        });
    }
}

#[derive(Debug, Clone)]
pub struct SignatureStatus {
    pub signature: Signature,
    pub err: Option<TransactionError>,
    pub memo: Option<String>,
}
