use thiserror::Error;

/// `Orby` (Orbital Observer) の操作中に発生する可能性のあるエラーを定義します。
#[derive(Error, Debug)]
pub enum OrbyError {
    /// メモリ不足
    #[error("Orby: Memory allocation failed: requested {requested_mb}MB, but only {available_mb}MB available.")]
    InsufficientMemory {
        requested_mb: u64,
        available_mb: u64,
    },

    /// レーン数不一致
    #[error("Orby: Lane count mismatch in pool '{pool_name}': expected {expected}, but received {found}.")]
    LaneCountMismatch {
        pool_name: String,
        expected: usize,
        found: usize,
    },

    /// 設定不一致
    #[error("Orby: Configuration mismatch for '{name}': {reason}")]
    ConfigMismatch { name: String, reason: String },

    /// 内部状態の矛盾
    #[error("Orby: Internal consistency check failed for '{name}': {message}")]
    InconsistentState { name: String, message: String },

    /// ストレージ（AOF）操作エラー
    #[error("Orby: Storage I/O error for '{name}': {source}")]
    StorageError {
        name: String,
        #[source]
        source: std::io::Error,
    },

    /// フォーマット不正
    #[error("Orby: Invalid data format in orbit: {0}")]
    InvalidFormat(String),

    /// ストレージ容量上限
    #[error("Orby: Storage is full in pool '{pool_name}': capacity is {capacity}.")]
    StorageFull { pool_name: String, capacity: usize },

    /// IOエラー
    #[error("Orby: I/O Error: {0}")]
    IoError(#[from] std::io::Error),

    /// その他カスタムエラー
    #[error("Orby: {0}")]
    Custom(String),
}
