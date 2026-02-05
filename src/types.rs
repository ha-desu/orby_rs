use std::path::PathBuf;

/// Storage strategy for Orby.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum SaveMode {
    /// In-memory only. No persistence.
    MemoryOnly,
    /// Specialized multi-lane persistence (Parallel Arrays persistence).
    Vault(Option<PathBuf>),
}

pub const HEADER_SIZE: u64 = 4096;
pub const STORAGE_MAGIC_V1: &[u8; 16] = b"ORBY_DATA_V1_LE ";
pub const DEFAULT_VAULT_DIR: &str = "vault_data";
pub const PULSE_SIZE: usize = 16;

/// Defines the physical behavior (data management strategy) of Orby.
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

/// `PulseCell` is the smallest 128-bit unit handled by Orby.
/// It has the exact same memory layout as `u128` (transparent).
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Serialize, serde::Deserialize,
)]
#[repr(transparent)]
pub struct PulseCell(u128); // 完全にプライベートにし、トレイト経由で操作

unsafe impl bytemuck::Pod for PulseCell {}
unsafe impl bytemuck::Zeroable for PulseCell {}

impl PulseCell {
    /// Creates a new `PulseCell` from a u128 value.
    #[inline]
    pub fn new(val: u128) -> Self {
        Self(val)
    }

    /// Creates a `PulseCell` with specific components.
    /// - `data`: Upper 120 bits
    /// - `lane_id`: Next 4 bits
    /// - `commit_cycle`: Lower 4 bits
    pub fn pack(data: u128, lane_id: u8, commit_cycle: u8) -> Self {
        let mask_data = (data >> 8) << 8;
        let mask_lane = ((lane_id & 0x0F) as u128) << 4;
        let mask_cycle = (commit_cycle & 0x0F) as u128;
        Self(mask_data | mask_lane | mask_cycle)
    }

    /// Returns the inner u128 value.
    #[inline]
    pub fn as_u128(&self) -> u128 {
        self.0
    }

    #[inline]
    pub fn data(&self) -> u128 {
        (self.0 >> 8) << 8
    }

    #[inline]
    pub fn lane_id(&self) -> u8 {
        ((self.0 >> 4) & 0x0F) as u8
    }

    #[inline]
    pub fn commit_cycle(&self) -> u8 {
        (self.0 & 0x0F) as u8
    }

    /// Updates the CommitCycle (lower 4 bits).
    pub fn with_commit_cycle(&self, cycle: u8) -> Self {
        let mask = 0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF0;
        let cycle_val = (cycle & 0x0F) as u128;
        Self((self.0 & mask) | cycle_val)
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
