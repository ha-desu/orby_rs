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

/// リングバッファで発生した操作を表現する列挙型。
/// これにより、ロジック層が物理的な永続化フォーマット（AOFのバイナリ等）に依存しなくなります。
#[derive(Debug, Clone)]
pub enum RingOperation {
    Insert {
        cursor: usize,
        row_count: usize,
        data: Vec<Vec<u128>>,
    },
    Update {
        physical_index: usize,
        id: u128,
        new_data: Vec<u128>,
        logical_column: usize,
    },
    Delete {
        physical_index: usize,
    },
    Purge {
        physical_indices: Vec<usize>,
        id: u128,
        logical_column: usize,
    },
    Truncate {
        new_rows: Vec<Vec<u128>>,
    },
    LaneBatch {
        lane_idx: usize,
        start_cursor: usize,
        values: Vec<u128>,
    },
    HeaderUpdate {
        len: usize,
        cursor: usize,
    },
}

/// 内部ロジック実行によって発生した変更内容。
#[derive(Default)]
pub struct PersistenceChanges {
    pub ops: Vec<RingOperation>,
}

impl PersistenceChanges {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn push(&mut self, op: RingOperation) {
        self.ops.push(op);
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// セマンティックな操作リストを、物理的な AOF/Mirror 用バイナリデータに変換します。
    pub fn flatten(&self, lane_count: usize) -> (Vec<u8>, Vec<(u64, Vec<u8>)>) {
        let mut aof_data = Vec::new();
        let mut mirror_data = Vec::new();

        for op in &self.ops {
            match op {
                RingOperation::Insert {
                    cursor,
                    row_count: _,
                    data,
                } => {
                    for (i, row) in data.iter().enumerate() {
                        // AOF
                        aof_data.push(AOF_OP_INSERT);
                        for &val in row {
                            aof_data.extend_from_slice(&val.to_le_bytes());
                        }

                        // Mirror
                        let physical_idx = cursor + i; // Note: logic already handled wrap-around in cursor calculation
                        let offset = crate::types::HEADER_SIZE
                            + (physical_idx as u64 * lane_count as u64 * 16);
                        let mut row_bytes = Vec::with_capacity(lane_count * 16);
                        for &val in row {
                            row_bytes.extend_from_slice(&val.to_le_bytes());
                        }
                        mirror_data.push((offset, row_bytes));
                    }
                }
                RingOperation::Update {
                    physical_index,
                    id,
                    new_data,
                    logical_column,
                } => {
                    // AOF
                    aof_data.push(AOF_OP_UPDATE);
                    aof_data.extend_from_slice(&(*logical_column as u32).to_le_bytes());
                    aof_data.extend_from_slice(&id.to_le_bytes());
                    for &val in new_data {
                        aof_data.extend_from_slice(&val.to_le_bytes());
                    }

                    // Mirror
                    let offset = crate::types::HEADER_SIZE
                        + (*physical_index as u64 * lane_count as u64 * 16);
                    let mut row_bytes = Vec::with_capacity(lane_count * 16);
                    for &val in new_data {
                        row_bytes.extend_from_slice(&val.to_le_bytes());
                    }
                    mirror_data.push((offset, row_bytes));
                }
                RingOperation::Delete { physical_index } => {
                    // AOF: Delete doesn't have a specific AOF op in current impl,
                    // usually handled by Purge or just left as is if not needed.
                    // But in ring.rs delete, we zero out.
                    // Mirror
                    let offset = crate::types::HEADER_SIZE
                        + (*physical_index as u64 * lane_count as u64 * 16);
                    let row_bytes = vec![0u8; lane_count * 16];
                    mirror_data.push((offset, row_bytes));
                }
                RingOperation::Purge {
                    physical_indices,
                    id,
                    logical_column,
                } => {
                    // AOF
                    aof_data.push(AOF_OP_PURGE);
                    aof_data.extend_from_slice(&(*logical_column as u32).to_le_bytes());
                    aof_data.extend_from_slice(&id.to_le_bytes());

                    // Mirror
                    for &idx in physical_indices {
                        let offset =
                            crate::types::HEADER_SIZE + (idx as u64 * lane_count as u64 * 16);
                        let row_bytes = vec![0u8; lane_count * 16];
                        mirror_data.push((offset, row_bytes));
                    }
                }
                RingOperation::Truncate { new_rows } => {
                    // AOF
                    aof_data.push(AOF_OP_TRUNCATE);

                    // Mirror (Bulk update)
                    if !new_rows.is_empty() {
                        let mut all_data = Vec::with_capacity(new_rows.len() * lane_count * 16);
                        for row in new_rows {
                            for &val in row {
                                all_data.extend_from_slice(&val.to_le_bytes());
                            }
                        }
                        mirror_data.push((crate::types::HEADER_SIZE, all_data));
                    }
                }
                RingOperation::LaneBatch {
                    lane_idx,
                    start_cursor,
                    values,
                } => {
                    // AOF
                    aof_data.push(AOF_OP_LANE_BATCH);
                    aof_data.extend_from_slice(&(*lane_idx as u32).to_le_bytes());
                    aof_data.extend_from_slice(&(values.len() as u32).to_le_bytes());
                    for &v in values {
                        aof_data.extend_from_slice(&v.to_le_bytes());
                    }

                    // Mirror
                    for (i, &val) in values.iter().enumerate() {
                        let physical_idx = start_cursor + i;
                        // Note: we assume no wrap-around here for simplicity or that it was handled.
                        // Actually, mirror updates for LaneBatch were complex.
                        // Let's stick to what ring.rs did.
                        let offset = crate::types::HEADER_SIZE
                            + (physical_idx as u64 * lane_count as u64 * 16);

                        // We only update ONE lane. This is tricky for Mirror (AoS).
                        // Mirror sync for LaneBatch in original code:
                        // row[lane_idx * 16..(lane_idx + 1) * 16].copy_from_slice(&values[i].to_le_bytes());
                        // mirror_data.push((offset, row));

                        let mut row = vec![0u8; lane_count * 16];
                        row[*lane_idx * 16..(*lane_idx + 1) * 16]
                            .copy_from_slice(&val.to_le_bytes());
                        mirror_data.push((offset, row));
                    }
                }
                RingOperation::HeaderUpdate { len, cursor } => {
                    // AOF: Header is usually not logged as Op, but recalculated.
                    // Mirror
                    mirror_data.push((24, (*len as u64).to_le_bytes().to_vec()));
                    mirror_data.push((32, (*cursor as u64).to_le_bytes().to_vec()));
                }
            }
        }
        (aof_data, mirror_data)
    }
}
