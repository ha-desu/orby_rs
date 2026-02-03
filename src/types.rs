use std::path::PathBuf;

/// Storage strategy for Orby.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SaveMode {
    /// In-memory only. No persistence.
    MemoryOnly,
    /// In-memory with full synchronization to a physical file. (Formerly Mirror)
    Sync(Option<PathBuf>),
    /// Disk-based access. Data is not loaded into memory buffer. (Formerly StorageOnly)
    Direct(Option<PathBuf>),
}

pub const HEADER_SIZE: u64 = 4096;
pub const STORAGE_MAGIC_V1: &[u8; 16] = b"ORBY_DATA_V1_LE ";

/// Orby の物理的な挙動（データ管理戦略）を定義します。
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum LogicMode {
    /// リングバッファモード：古いデータを自動的に上書きします。時系列ログやタイムライン向け。
    Ring,
    /// 固定制限モード：最大キャパシティに達すると挿入がエラーになります。削除は Swap-Remove で行われます。モンスター管理などの個体数制限向け。
    Fixed,
}

impl LogicMode {
    pub fn as_u8(&self) -> u8 {
        match self {
            LogicMode::Ring => 0,
            LogicMode::Fixed => 1,
        }
    }

    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(LogicMode::Ring),
            1 => Some(LogicMode::Fixed),
            _ => None,
        }
    }
}

/// Orbyが扱う最小単位、128-bitのデータフィールド。
/// メモリレイアウトは u128 と完全に同一(transparent)です。
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct OrbitField(u128); // 完全にプライベートにし、トレイト経由で操作

impl OrbitField {
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
impl From<u128> for OrbitField {
    #[inline]
    fn from(v: u128) -> Self {
        Self(v)
    }
}

impl From<OrbitField> for u128 {
    #[inline]
    fn from(b: OrbitField) -> Self {
        b.0
    }
}
