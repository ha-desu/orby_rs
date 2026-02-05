use crate::logic::OrbyRingBufferSilo;
use crate::types::{LogicMode, PulseCell};
use parking_lot::RwLockReadGuard;
use std::sync::Arc;

/// Orby のデータを一件ずつ、最新順にスキャンするためのイテレータ。
///
/// ### 【警告】デッドロックと書き込みブロック
/// このイテレータは内部で読み取りロック（`RwLockReadGuard`）を保持し続けます。
/// そのため、このイテレータが存続している間は、`insert` や `purge` などの
/// 書き込み操作（`RwLock::write` を必要とする操作）はすべてブロックされます。
///
/// **原則として、`.collect()` などでサッと回して、すぐにドロップするようにしてください。**
/// 長時間のループ内や非同期境界を跨いでの保持は推奨されません。
pub struct OrbyIterator<'a, F> {
    pub(crate) store: RwLockReadGuard<'a, OrbyRingBufferSilo>,
    pub(crate) filter: F,
    pub(crate) current_idx: usize,
    pub(crate) logic_mode: LogicMode,
    pub(crate) cursor: usize,
    pub(crate) cap: usize,
    pub(crate) len: usize,
    pub(crate) file: Option<std::fs::File>,
}

impl<'a, F> Iterator for OrbyIterator<'a, F>
where
    F: Fn(&[PulseCell]) -> bool,
{
    type Item = Arc<[u128]>;

    fn next(&mut self) -> Option<Self::Item> {
        let dim = self.store.ring_buffer_lane_count;

        while self.current_idx < self.len {
            let i = self.current_idx;
            self.current_idx += 1;

            let physical_idx = match self.logic_mode {
                LogicMode::RingBuffer => {
                    if self.cursor > i {
                        self.cursor - 1 - i
                    } else {
                        self.cap + self.cursor - 1 - i
                    }
                }
            };

            let row_data_vec: Vec<PulseCell> = if let Some(ref mut f) = self.file {
                use std::io::{Read, Seek, SeekFrom};
                // strideは廃止、ring_buffer_lane_countを使用
                let offset = crate::types::HEADER_SIZE + (physical_idx * dim * 16) as u64;
                if f.seek(SeekFrom::Start(offset)).is_err() {
                    break;
                }
                let mut buf = vec![0u8; dim * 16];
                if f.read_exact(&mut buf).is_err() {
                    break;
                }
                buf.chunks_exact(16)
                    .map(|c| PulseCell::new(u128::from_le_bytes(c.try_into().unwrap())))
                    .collect()
            } else {
                // AoS 再構築
                if self.store.lanes.is_empty() {
                    break;
                }
                let mut row = Vec::with_capacity(dim);
                for col in 0..dim {
                    row.push(self.store.lanes[col].buffer[physical_idx]);
                }
                row
            };

            let row_data = &row_data_vec;

            // 全てが 0 の行はスキップ
            if row_data.iter().all(|v| v.as_u128() == 0) {
                continue;
            }

            if (self.filter)(row_data) {
                return Some(Arc::from(
                    row_data.iter().map(|b| b.as_u128()).collect::<Vec<_>>(),
                ));
            }
        }
        None
    }
}
