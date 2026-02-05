pub mod ring;

use crate::types::{LogicMode, PulseCell, SaveMode};

/// 各次元（カラム）のデータを保持する独立したリングバッファ。
pub struct OrbyRingBuffer {
    pub buffer: Vec<PulseCell>,
}

impl OrbyRingBuffer {
    pub fn new(capacity: usize) -> Self {
        Self {
            buffer: vec![PulseCell::new(0); capacity],
        }
    }
}

/// `Orby` の内部状態を保持する構造体。
pub struct OrbyRingBufferSilo {
    pub name: String,
    pub lanes: Vec<OrbyRingBuffer>,
    pub cursor: usize,
    pub len: usize,
    pub capacity: usize,
    pub ring_buffer_lane_count: usize,
    pub compaction: bool,
    pub logic_mode: LogicMode,
    pub storage_mode: SaveMode,
    pub(crate) aof_sender: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
    pub(crate) mirror_sender: Option<tokio::sync::mpsc::Sender<Vec<(u64, Vec<u8>)>>>,
    pub(crate) mirror_path: Option<std::path::PathBuf>,
    pub(crate) vault_path: Option<std::path::PathBuf>,
}

pub const AOF_OP_INSERT: u8 = 0x01;
pub const AOF_OP_PURGE: u8 = 0x02;
pub const AOF_OP_UPDATE: u8 = 0x03;
pub const AOF_OP_TRUNCATE: u8 = 0x04;
pub const AOF_OP_LANE_BATCH: u8 = 0x05;
