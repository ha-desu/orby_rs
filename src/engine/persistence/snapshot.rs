use crate::engine::Orby;
use crate::error::OrbyError;
use crate::types::{PulseCell, STORAGE_MAGIC_V1};
use std::io::Write;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

impl Orby {
    /// Orbyバイナリファイルを明示的に指定してリストアします。
    pub(crate) async fn restore_from_storage(&self, path: PathBuf) -> Result<(), OrbyError> {
        let mut file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| OrbyError::IoError(format!("Failed to open restore file: {}", e)))?;

        let mut header = [0u8; crate::types::HEADER_SIZE as usize];
        file.read_exact(&mut header)
            .await
            .map_err(|e| OrbyError::IoError(e.to_string()))?;

        if &header[0..16] != STORAGE_MAGIC_V1 {
            return Err(OrbyError::IoError("Invalid magic number".into()));
        }

        let l_cap = u64::from_le_bytes(header[16..24].try_into().unwrap()) as usize;
        let l_len = u64::from_le_bytes(header[24..32].try_into().unwrap()) as usize;
        let l_cursor = u64::from_le_bytes(header[32..40].try_into().unwrap()) as usize;
        let l_dim = u32::from_le_bytes(header[40..44].try_into().unwrap()) as usize;
        let _l_pdim = u32::from_le_bytes(header[44..48].try_into().unwrap()) as usize;
        let l_logic_u8 = header[48];

        {
            let mut store = self.inner.write();
            let l_logic = crate::types::LogicMode::from_u8(l_logic_u8)
                .ok_or_else(|| OrbyError::IoError("Invalid logic mode in file".into()))?;

            if l_cap != store.capacity || l_dim != store.ring_buffer_lane_count {
                return Err(OrbyError::LaneCountMismatch {
                    pool_name: store.name.clone(),
                    expected: store.ring_buffer_lane_count,
                    found: l_dim,
                });
            }

            if l_logic != store.logic_mode {
                return Err(OrbyError::IoError("Logic mode mismatch".into()));
            }

            store.len = l_len;
            store.cursor = l_cursor;

            if !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty() {
                // ファイルはAoS形式、メモリはSoA形式
                // load stride がある場合、それに従うべきだが、今回はデータサイズ計算を dim * 16 とする（密）
                // もし互換性を厳密にするなら l_pdim を使うべき。
                let data_size = (store.capacity * l_dim * 16) as usize;
                let mut data_buf = vec![0u8; data_size];
                file.read_exact(&mut data_buf)
                    .await
                    .map_err(|e| OrbyError::IoError(e.to_string()))?;

                // data_buf is [row0_col0, row0_col1, ..., row1_col0, ...]
                // We need to distribute to lanes[col].buffer[row]
                let dim = store.ring_buffer_lane_count;
                for (i, chunk) in data_buf.chunks_exact(16).enumerate() {
                    let row_idx = i / dim;
                    let col_idx = i % dim;
                    let val = u128::from_le_bytes(chunk.try_into().unwrap());
                    store.lanes[col_idx].buffer[row_idx] = PulseCell::new(val);
                }
            }
        }

        Ok(())
    }

    /// 指定されたファイルパスに 4KB ヘッダー付きのスナップショットを保存します。
    pub async fn write_snapshot_to_file(&self, path: PathBuf) -> Result<(), String> {
        let (name, capacity, len, cursor, dim, logic) = {
            let store = self.inner.read();
            (
                store.name.clone(),
                store.capacity,
                store.len,
                store.cursor,
                store.ring_buffer_lane_count,
                store.logic_mode,
            )
        };

        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            let store = this.inner.read();
            let file = std::fs::File::create(&path).map_err(|e| e.to_string())?;
            let mut writer = std::io::BufWriter::new(file);

            let mut header = vec![0u8; crate::types::HEADER_SIZE as usize];
            header[0..16].copy_from_slice(STORAGE_MAGIC_V1);
            header[16..24].copy_from_slice(&(capacity as u64).to_le_bytes());
            header[24..32].copy_from_slice(&(len as u64).to_le_bytes());
            header[32..40].copy_from_slice(&(cursor as u64).to_le_bytes());
            header[40..44].copy_from_slice(&(dim as u32).to_le_bytes());
            header[44..48].copy_from_slice(&(dim as u32).to_le_bytes());
            header[48] = logic.as_u8();

            let name_bytes = name.as_bytes();
            let name_len = name_bytes.len().min(64);
            header[64..64 + name_len].copy_from_slice(&name_bytes[..name_len]);

            writer.write_all(&header).map_err(|e| e.to_string())?;

            if !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty() {
                for i in 0..capacity {
                    for col in 0..dim {
                        let val = store.lanes[col].buffer[i].as_u128();
                        writer
                            .write_all(&val.to_le_bytes())
                            .map_err(|e| e.to_string())?;
                    }
                }
            }

            writer.flush().map_err(|e| e.to_string())?;
            writer.get_mut().sync_all().map_err(|e| e.to_string())?;
            Ok::<(), String>(())
        })
        .await
        .map_err(|e| e.to_string())?
    }
}
