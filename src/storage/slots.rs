use {
    crate::{
        metrics::STORAGE_STORED_SLOTS,
        util::{HashMap, HashSet},
    },
    metrics::{Counter, counter},
    solana_sdk::clock::Slot,
    std::{
        ops::Deref,
        sync::{
            Arc, Mutex,
            atomic::{AtomicBool, AtomicU64, Ordering},
        },
    },
};

#[derive(Debug)]
pub struct StoredSlotsInner {
    processed: AtomicU64,
    processed_counter: Counter,
    confirmed: AtomicU64,
    confirmed_counter: Counter,
    finalized: AtomicU64,
    finalized_counter: Counter,
    first_available: AtomicU64,
    first_available_counter: Counter,
    max_recent_blockhashes: AtomicBool,
}

impl Default for StoredSlotsInner {
    fn default() -> Self {
        Self {
            processed: AtomicU64::new(u64::MIN),
            processed_counter: counter!(STORAGE_STORED_SLOTS, "commitment" => "processed"),
            confirmed: AtomicU64::new(u64::MIN),
            confirmed_counter: counter!(STORAGE_STORED_SLOTS, "commitment" => "confirmed"),
            finalized: AtomicU64::new(u64::MIN),
            finalized_counter: counter!(STORAGE_STORED_SLOTS, "commitment" => "finalized"),
            first_available: AtomicU64::new(u64::MAX),
            first_available_counter: counter!(STORAGE_STORED_SLOTS, "commitment" => "first_available"),
            max_recent_blockhashes: AtomicBool::new(false),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct StoredSlots {
    inner: Arc<StoredSlotsInner>,
}

impl Deref for StoredSlots {
    type Target = StoredSlotsInner;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl StoredSlots {
    pub fn is_ready(&self) -> bool {
        self.processed_load() != u64::MIN
            && self.confirmed_load() != u64::MIN
            && self.finalized_load() != u64::MIN
            && self.first_available_load() != u64::MAX
            && self.max_recent_blockhashes.load(Ordering::Relaxed)
    }

    pub fn processed_load(&self) -> Slot {
        self.processed.load(Ordering::SeqCst)
    }

    pub fn processed_store(&self, slot: Slot) {
        self.processed.store(slot, Ordering::SeqCst);
        self.processed_counter.absolute(slot);
    }

    pub fn confirmed_load(&self) -> Slot {
        self.confirmed.load(Ordering::SeqCst)
    }

    pub fn confirmed_store(&self, slot: Slot) {
        self.confirmed.store(slot, Ordering::SeqCst);
        self.confirmed_counter.absolute(slot);
    }

    pub fn finalized_load(&self) -> Slot {
        self.finalized.load(Ordering::Relaxed)
    }

    pub fn finalized_store(&self, slot: Slot) {
        self.finalized.store(slot, Ordering::Relaxed);
        self.finalized_counter.absolute(slot);
    }

    pub fn first_available_load(&self) -> Slot {
        self.first_available.load(Ordering::SeqCst)
    }

    pub fn first_available_store(&self, slot: Option<Slot>) {
        let slot = slot.unwrap_or(u64::MAX);
        self.first_available.store(slot, Ordering::SeqCst);
        self.first_available_counter.absolute(slot);
    }
}

#[derive(Debug, Clone)]
pub struct StoredSlotsRead {
    stored_slots: StoredSlots,
    slots_confirmed: Arc<Mutex<HashMap<Slot, HashSet<usize>>>>,
    slots_finalized: Arc<Mutex<HashMap<Slot, HashSet<usize>>>>,
    max_recent_blockhashes: Arc<Mutex<usize>>,
    max_recent_blockhashes_ready: bool,
    total_readers: usize,
}

impl StoredSlotsRead {
    pub fn new(stored_slots: StoredSlots, total_readers: usize) -> Self {
        Self {
            stored_slots,
            slots_confirmed: Arc::default(),
            slots_finalized: Arc::default(),
            max_recent_blockhashes: Arc::default(),
            max_recent_blockhashes_ready: false,
            total_readers,
        }
    }

    fn set(
        &self,
        map: &Arc<Mutex<HashMap<Slot, HashSet<usize>>>>,
        index: usize,
        slot: Slot,
    ) -> bool {
        let mut lock = map.lock().expect("unpanicked mutex");

        let entry = lock.entry(slot).or_default();
        entry.insert(index);

        if entry.len() == self.total_readers {
            lock.remove(&slot);
            true
        } else {
            false
        }
    }

    pub fn set_confirmed(&self, index: usize, slot: Slot) {
        if self.set(&self.slots_confirmed, index, slot) {
            self.stored_slots.confirmed_store(slot);
        }
    }

    pub fn set_finalized(&self, index: usize, slot: Slot) {
        if self.set(&self.slots_finalized, index, slot) {
            self.stored_slots.finalized_store(slot);
        }
    }

    // max recent blockhashes
    pub fn set_ready(&mut self, ready: bool) {
        if ready && !self.max_recent_blockhashes_ready {
            self.max_recent_blockhashes_ready = true;

            let mut lock = self
                .max_recent_blockhashes
                .lock()
                .expect("unpanicked mutex");
            *lock += 1;

            if *lock == self.total_readers {
                self.stored_slots
                    .max_recent_blockhashes
                    .store(true, Ordering::Relaxed);
            }
        }
    }
}
