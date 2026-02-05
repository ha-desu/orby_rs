use std::path::PathBuf;

/// Storage strategy for Orby.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SaveMode {
    /// In-memory only. No persistence.
    MemoryOnly,
    /// マルチレーンの特殊ストレージ (並列アレイの永続性)。
    Vault(Option<PathBuf>),
}

pub const HEADER_SIZE: u64 = 4096;
pub const STORAGE_MAGIC_V1: &[u8; 16] = b"ORBY_DATA_V1_LE ";

/// Orby の物理的な挙動（データ管理戦略）を定義します。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LogicMode {
    RingBuffer,
}

impl LogicMode {
    pub fn as_u8(&self) -> u8 {
        match self {
            LogicMode::RingBuffer => 0,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(LogicMode::RingBuffer),
            _ => None,
        }
    }
}

/// PulseCell は Orby が扱う最小単位の 128-bit データフィールドです。
/// メモリレイアウトは u128 と完全に同一(transparent)です。
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct PulseCell(u128); // 完全にプライベートにし、トレイト経由で操作

impl PulseCell {
    #[inline]
    pub fn new(val: u128) -> Self {
        Self(val)
    }

    #[inline]
    pub fn as_u128(&self) -> u128 {
        self.0
    }
}

// 変換トレイトの実装（これが汎用性の鍵！）
impl From<u128> for PulseCell {
    #[inline]
    fn from(v: u128) -> Self {
        Self(v)
    }
}

impl From<PulseCell> for u128 {
    #[inline]
    fn from(b: PulseCell) -> Self {
        b.0
    }
}
