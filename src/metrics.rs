use {
    crate::{config::ConfigMetrics, version::VERSION as VERSION_INFO},
    prometheus::{IntCounterVec, IntGaugeVec, Opts, Registry},
    solana_sdk::{clock::Slot, commitment_config::CommitmentLevel},
    std::{future::Future, sync::Once},
    tokio::task::JoinError,
};

lazy_static::lazy_static! {
    pub static ref REGISTRY: Registry = Registry::new();

    static ref VERSION: IntCounterVec = IntCounterVec::new(
        Opts::new("version", "Alpamayo version info"),
        &["buildts", "git", "package", "proto_dragonsmouth", "proto_richat", "rustc", "solana", "version"]
    ).unwrap();

    static ref STORAGE_STORED_SLOTS: IntGaugeVec = IntGaugeVec::new(
        Opts::new("storage_stored_slots", "Stored slots in db"),
        &["commitment"]
    ).unwrap();
}

pub async fn spawn_server(
    config: ConfigMetrics,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<impl Future<Output = Result<(), JoinError>>> {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        macro_rules! register {
            ($collector:ident) => {
                REGISTRY
                    .register(Box::new($collector.clone()))
                    .expect("collector can't be registered");
            };
        }
        register!(VERSION);
        register!(STORAGE_STORED_SLOTS);

        VERSION
            .with_label_values(&[
                VERSION_INFO.buildts,
                VERSION_INFO.git,
                VERSION_INFO.package,
                VERSION_INFO.proto,
                VERSION_INFO.proto_richat,
                VERSION_INFO.rustc,
                VERSION_INFO.solana,
                VERSION_INFO.version,
            ])
            .inc();
    });

    richat_shared::metrics::spawn_server(
        richat_shared::config::ConfigMetrics {
            endpoint: config.endpoint,
        },
        || REGISTRY.gather(),
        shutdown,
    )
    .await
    .map_err(Into::into)
}

fn commitment_as_label(commitment: CommitmentLevel) -> &'static str {
    match commitment {
        CommitmentLevel::Processed => "processed",
        CommitmentLevel::Confirmed => "confirmed",
        CommitmentLevel::Finalized => "finalized",
    }
}

pub fn storage_stored_slots_set_commitment(slot: Slot, commitment: CommitmentLevel) {
    let labels = &[commitment_as_label(commitment)];
    if slot == u64::MIN {
        let _ = STORAGE_STORED_SLOTS.remove_label_values(labels);
    } else {
        STORAGE_STORED_SLOTS
            .with_label_values(labels)
            .set(slot as i64);
    }
}

pub fn storage_stored_slots_set_first_available(slot: Slot) {
    let labels = &["first_available"];
    if slot == u64::MIN {
        let _ = STORAGE_STORED_SLOTS.remove_label_values(labels);
    } else {
        STORAGE_STORED_SLOTS
            .with_label_values(labels)
            .set(slot as i64);
    }
}
