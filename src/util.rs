pub type HashMap<K, V> = std::collections::HashMap<K, V, foldhash::quality::RandomState>;
pub type HashSet<K> = std::collections::HashSet<K, foldhash::quality::RandomState>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VecSide {
    Back,
    Front,
}
