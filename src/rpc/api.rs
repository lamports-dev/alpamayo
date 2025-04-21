pub const X_ERROR: &str = "x-error";
pub const X_SLOT: &str = "x-slot";

pub fn check_call_support<T: Eq + std::fmt::Debug>(calls: &[T], call: T) -> anyhow::Result<bool> {
    let count = calls.iter().filter(|value| **value == call).count();
    anyhow::ensure!(count <= 1, "{call:?} defined multiple times");
    Ok(count == 1)
}
