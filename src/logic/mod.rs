pub mod fixed;
pub mod ring;

use crate::types::{LogicMode, OrbitField, SaveMode};

/// `Orby` の内部状態を保持する構造体。
pub struct OrbyStore {
    pub name: String,
    pub buffer: Vec<OrbitField>,
    pub head: usize,
    pub len: usize,
    pub capacity: usize,
    pub dimension: usize,
    pub padded_dimension: usize,
    pub logic_mode: LogicMode,
    pub storage_mode: SaveMode,
    pub(crate) aof_sender: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
    pub(crate) mirror_sender: Option<tokio::sync::mpsc::Sender<Vec<(u64, Vec<u8>)>>>,
    pub(crate) mirror_path: Option<std::path::PathBuf>,
}

pub const AOF_OP_INSERT: u8 = 0x01;
pub const AOF_OP_PURGE: u8 = 0x02;
pub const AOF_OP_UPDATE: u8 = 0x03;
pub const AOF_OP_TRUNCATE: u8 = 0x04;
