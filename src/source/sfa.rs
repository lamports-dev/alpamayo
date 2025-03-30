use {
    crate::storage::rocksdb::SfaIndex,
    solana_sdk::{pubkey::Pubkey, signature::Signature, transaction::TransactionError},
};

#[derive(Debug)]
pub struct SignatureForAddress {
    pub hash: [u8; 8],
    pub address: Pubkey,
    pub signature: Signature,
    pub err: Option<TransactionError>,
    pub memo: Option<String>,
}

impl SignatureForAddress {
    pub fn new(
        address: Pubkey,
        signature: Signature,
        err: Option<TransactionError>,
        memo: Option<String>,
    ) -> Self {
        let hash = SfaIndex::key(&address);

        Self {
            hash,
            address,
            signature,
            err,
            memo,
        }
    }
}

#[derive(Debug)]
pub struct SignaturesForAddress {
    pub hash: [u8; 8],
    pub address: Pubkey,
    pub signatures: Vec<SignatureStatus>,
}

impl SignaturesForAddress {
    pub fn new(sfa: SignatureForAddress) -> Self {
        Self {
            hash: sfa.hash,
            address: sfa.address,
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

#[derive(Debug)]
pub struct SignatureStatus {
    pub signature: Signature,
    pub err: Option<TransactionError>,
    pub memo: Option<String>,
}
