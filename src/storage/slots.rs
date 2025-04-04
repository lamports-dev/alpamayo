use {
    crate::metrics,
    solana_sdk::{clock::Slot, commitment_config::CommitmentLevel},
    std::{
        collections::{HashMap, HashSet},
        sync::{
            Arc, Mutex,
            atomic::{AtomicU64, Ordering},
        },
    },
};

#[derive(Debug, Clone)]
pub struct StoredSlots {
    processed: Arc<AtomicU64>,
    confirmed: Arc<AtomicU64>,
    finalized: Arc<AtomicU64>,
    first_available: Arc<AtomicU64>,
}

impl Default for StoredSlots {
    fn default() -> Self {
        Self {
            processed: Arc::new(AtomicU64::new(u64::MIN)),
            confirmed: Arc::new(AtomicU64::new(u64::MIN)),
            finalized: Arc::new(AtomicU64::new(u64::MIN)),
            first_available: Arc::new(AtomicU64::new(u64::MAX)),
        }
    }
}

impl StoredSlots {
    pub fn is_ready(&self) -> bool {
        self.processed_load() != u64::MIN
            && self.confirmed_load() != u64::MIN
            && self.finalized_load() != u64::MIN
            && self.first_available_load() != u64::MAX
    }

    pub fn processed_load(&self) -> Slot {
        self.processed.load(Ordering::SeqCst)
    }

    pub fn processed_store(&self, slot: Slot) {
        self.processed.store(slot, Ordering::SeqCst);
        metrics::storage_stored_slots_set_commitment(slot, CommitmentLevel::Processed);
    }

    pub fn confirmed_load(&self) -> Slot {
        self.confirmed.load(Ordering::SeqCst)
    }

    pub fn confirmed_store(&self, slot: Slot) {
        self.confirmed.store(slot, Ordering::SeqCst);
        metrics::storage_stored_slots_set_commitment(slot, CommitmentLevel::Confirmed);
    }

    pub fn finalized_load(&self) -> Slot {
        self.finalized.load(Ordering::Relaxed)
    }

    pub fn finalized_store(&self, slot: Slot) {
        self.finalized.store(slot, Ordering::Relaxed);
        metrics::storage_stored_slots_set_commitment(slot, CommitmentLevel::Finalized);
    }

    pub fn first_available_load(&self) -> Slot {
        self.first_available.load(Ordering::SeqCst)
    }

    pub fn first_available_store(&self, slot: Option<Slot>) {
        let slot = slot.unwrap_or(u64::MAX);
        self.first_available.store(slot, Ordering::SeqCst);
        metrics::storage_stored_slots_set_first_available(slot);
    }
}

#[derive(Debug, Clone)]
pub struct StoredConfirmedSlot {
    stored_slots: StoredSlots,
    slots: Arc<Mutex<HashMap<Slot, HashSet<usize>>>>,
    total_readers: usize,
}

impl StoredConfirmedSlot {
    pub fn new(stored_slots: StoredSlots, total_readers: usize) -> Self {
        Self {
            stored_slots,
            slots: Arc::default(),
            total_readers,
        }
    }

    pub fn set_confirmed(&self, index: usize, slot: Slot) {
        let mut lock = self.slots.lock().expect("unpanicked mutex");

        let entry = lock.entry(slot).or_default();
        entry.insert(index);

        if entry.len() == self.total_readers {
            self.stored_slots.confirmed_store(slot);
            lock.remove(&slot);
        }
    }
}
