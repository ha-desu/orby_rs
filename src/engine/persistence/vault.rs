use crate::engine::Orby;
use crate::error::OrbyError;
use crate::types::STORAGE_MAGIC_V1;
use std::fs::{File, OpenOptions};
use std::os::unix::fs::FileExt;

/// `Orby Vault System` は、多次元配列（Parallel Arrays）の各次元を
/// 独立したバイナリファイルとして管理するストレージエンジンです。
pub struct OrbyVault;

impl Orby {
    /// 指定されたパスを Vault ディレクトリとして初期化します。
    pub(crate) async fn init_vault(&self) -> Result<(), OrbyError> {
        let (vault_path, capacity, ring_buffer_lane_count) = {
            let store = self.inner.read();
            let p = store
                .vault_path
                .clone()
                .ok_or_else(|| OrbyError::IoError("Vault path is not set".into()))?;
            (p, store.capacity, store.ring_buffer_lane_count)
        };

        if !vault_path.exists() {
            tokio::fs::create_dir_all(&vault_path)
                .await
                .map_err(|e| OrbyError::IoError(format!("Failed to create vault dir: {}", e)))?;
        }

        // 1. Initialize Lane Files
        for i in 0..ring_buffer_lane_count {
            let lane_path = vault_path.join(format!("lane_{}.bin", i));
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&lane_path)
                .await
                .map_err(|e| {
                    OrbyError::IoError(format!("Failed to create lane file {}: {}", i, e))
                })?;
            file.set_len((capacity * 16) as u64)
                .await
                .map_err(|e| OrbyError::IoError(e.to_string()))?;
            file.sync_all()
                .await
                .map_err(|e| OrbyError::IoError(e.to_string()))?;
        }

        // 2. Initialize Header File
        let header_path = vault_path.join("header.bin");
        let mut header_file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&header_path)
            .await
            .map_err(|e| OrbyError::IoError(format!("Failed to create header file: {}", e)))?;

        let mut header_data = vec![0u8; 4096];
        header_data[0..16].copy_from_slice(STORAGE_MAGIC_V1);
        header_data[16..24].copy_from_slice(&(capacity as u64).to_le_bytes());
        header_data[24..32].copy_from_slice(&(0u64).to_le_bytes()); // len
        header_data[32..40].copy_from_slice(&(0u64).to_le_bytes()); // cursor
        header_data[40..44].copy_from_slice(&(ring_buffer_lane_count as u32).to_le_bytes()); // lane_count

        use tokio::io::AsyncWriteExt;
        header_file
            .write_all(&header_data)
            .await
            .map_err(|e| OrbyError::IoError(e.to_string()))?;
        header_file
            .sync_all()
            .await
            .map_err(|e| OrbyError::IoError(e.to_string()))?;

        Ok(())
    }

    /// Vault からデータを復元します。
    pub(crate) async fn restore_from_vault(&self) -> Result<(), OrbyError> {
        let vault_path = {
            let store = self.inner.read();
            store
                .vault_path
                .clone()
                .ok_or_else(|| OrbyError::IoError("Vault path is not set".into()))?
        };

        let header_path = vault_path.join("header.bin");
        let mut header_file = tokio::fs::File::open(&header_path)
            .await
            .map_err(|e| OrbyError::IoError(format!("Failed to open header.bin: {}", e)))?;

        let mut header_data = [0u8; 4096];
        use tokio::io::AsyncReadExt;
        header_file
            .read_exact(&mut header_data)
            .await
            .map_err(|e| OrbyError::IoError(e.to_string()))?;

        if &header_data[0..16] != STORAGE_MAGIC_V1 {
            return Err(OrbyError::IoError(
                "Invalid magic number in header.bin".into(),
            ));
        }

        let v_cap = u64::from_le_bytes(header_data[16..24].try_into().unwrap()) as usize;
        let v_len = u64::from_le_bytes(header_data[24..32].try_into().unwrap()) as usize;
        let v_cursor = u64::from_le_bytes(header_data[32..40].try_into().unwrap()) as usize;
        let v_dim = u32::from_le_bytes(header_data[40..44].try_into().unwrap()) as usize;

        {
            let mut store = self.inner.write();
            if v_cap != store.capacity || v_dim != store.ring_buffer_lane_count {
                return Err(OrbyError::LaneCountMismatch {
                    pool_name: store.name.clone(),
                    expected: store.ring_buffer_lane_count,
                    found: v_dim,
                });
            }

            // Size validation for lanes
            for i in 0..v_dim {
                let lane_path = vault_path.join(format!("lane_{}.bin", i));
                let metadata = std::fs::metadata(&lane_path).map_err(|e| {
                    OrbyError::IoError(format!("Lane {} missing or inaccessible: {}", i, e))
                })?;
                if metadata.len() != (v_cap * 16) as u64 {
                    return Err(OrbyError::IoError(format!(
                        "Lane {} size mismatch. Expected {}, found {}",
                        i,
                        v_cap * 16,
                        metadata.len()
                    )));
                }
            }

            store.len = v_len;
            store.cursor = v_cursor;

            // Load to memory if not Direct mode
            if !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty() {
                for i in 0..v_dim {
                    let lane_path = vault_path.join(format!("lane_{}.bin", i));
                    let mut f = std::fs::File::open(&lane_path)
                        .map_err(|e| OrbyError::IoError(e.to_string()))?;

                    // Bulk read lane
                    let mut buf = vec![0u8; v_cap * 16];
                    use std::io::Read;
                    f.read_exact(&mut buf)
                        .map_err(|e| OrbyError::IoError(e.to_string()))?;

                    for (row, chunk) in buf.chunks_exact(16).enumerate() {
                        let val = u128::from_le_bytes(chunk.try_into().unwrap());
                        store.lanes[i].buffer[row] = crate::types::PulseCell::new(val);
                    }
                }
            }
        }

        Ok(())
    }

    /// 改ざん防止・アトミック書き込みを伴うコミット処理。
    /// 手順: 1. 全レーン書き込み -> 2. fsync(Lanes) -> 3. ヘッダ更新 -> 4. fsync(Header)
    pub(crate) async fn commit_vault_batch(
        &self,
        index: usize,
        rows: Vec<Vec<u128>>,
    ) -> Result<(), OrbyError> {
        let (vault_path, ring_buffer_lane_count, capacity) = {
            let store = self.inner.read();
            (
                store.vault_path.clone().unwrap(),
                store.ring_buffer_lane_count,
                store.capacity,
            )
        };

        tokio::task::spawn_blocking(move || {
            // 1. Write to all lanes
            let lane_files: Vec<std::fs::File> = (0..ring_buffer_lane_count)
                .map(|i| {
                    let p = vault_path.join(format!("lane_{}.bin", i));
                    std::fs::OpenOptions::new()
                        .write(true)
                        .open(p)
                        .map_err(|e| OrbyError::IoError(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;

            for (row_offset, row) in rows.iter().enumerate() {
                let physical_idx = (index + row_offset) % capacity;
                for (col, &val) in row.iter().enumerate() {
                    if col < ring_buffer_lane_count {
                        let bytes = val.to_le_bytes();
                        lane_files[col]
                            .write_at(&bytes, (physical_idx * 16) as u64)
                            .map_err(|e| OrbyError::IoError(e.to_string()))?;
                    }
                }
            }

            // 2. fsync all lanes
            for f in &lane_files {
                f.sync_all()
                    .map_err(|e| OrbyError::IoError(e.to_string()))?;
            }
            Ok::<(), OrbyError>(())
        })
        .await
        .map_err(|e| OrbyError::IoError(e.to_string()))??;

        // 3. Update Header
        self.commit_vault_header().await?;

        Ok(())
    }

    /// 特定のレーンに対してバルク書き込みを行い、他レーンを垂直同期（ゼロクリア）します。
    pub(crate) async fn commit_vault_lane_batch(
        &self,
        lane_idx: usize,
        start_index: usize,
        values: Vec<u128>,
    ) -> Result<(), OrbyError> {
        let (vault_path, ring_buffer_lane_count, capacity) = {
            let store = self.inner.read();
            (
                store.vault_path.clone().unwrap(),
                store.ring_buffer_lane_count,
                store.capacity,
            )
        };

        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            // 1. ターゲットレーンへのバルク I/O
            {
                let p = vault_path.join(format!("lane_{}.bin", lane_idx));
                let f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(p)
                    .map_err(|e| OrbyError::IoError(e.to_string()))?;

                this.bulk_write_lane_file(&f, start_index, capacity, &values)?;
                f.sync_all()
                    .map_err(|e| OrbyError::IoError(e.to_string()))?;
            }

            // 2. 他の全てのレーンに対して、同じインデックス範囲をゼロ埋め（垂直同期）
            let zeros = vec![0u128; values.len()];
            for col in 0..ring_buffer_lane_count {
                if col == lane_idx {
                    continue;
                }
                let p = vault_path.join(format!("lane_{}.bin", col));
                let f = std::fs::OpenOptions::new()
                    .write(true)
                    .open(p)
                    .map_err(|e| OrbyError::IoError(e.to_string()))?;

                this.bulk_write_lane_file(&f, start_index, capacity, &zeros)?;
                f.sync_all()
                    .map_err(|e| OrbyError::IoError(e.to_string()))?;
            }
            Ok::<(), OrbyError>(())
        })
        .await
        .map_err(|e| OrbyError::IoError(e.to_string()))??;

        // 3. ヘッダー更新
        self.commit_vault_header().await?;

        Ok(())
    }

    /// リングバッファのラップアラウンドを考慮したバルク書き込みヘルパー
    fn bulk_write_lane_file(
        &self,
        f: &File,
        start_idx: usize,
        capacity: usize,
        values: &[u128],
    ) -> Result<(), OrbyError> {
        let count = values.len();
        let end_idx = start_idx + count;

        if end_idx <= capacity {
            // ケースA: ラップアラウンドなし。単一の I/O
            let bytes: Vec<u8> = values.iter().flat_map(|v| v.to_le_bytes()).collect();
            f.write_at(&bytes, (start_idx * 16) as u64)
                .map_err(|e| OrbyError::IoError(e.to_string()))?;
        } else {
            // ケースB: ラップアラウンドあり。2回に分割
            let len_to_end = capacity - start_idx;

            // 1. 末尾まで
            let bytes1: Vec<u8> = values[..len_to_end]
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect();
            f.write_at(&bytes1, (start_idx * 16) as u64)
                .map_err(|e| OrbyError::IoError(e.to_string()))?;

            // 2. 先頭から
            let bytes2: Vec<u8> = values[len_to_end..]
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect();
            f.write_at(&bytes2, 0)
                .map_err(|e| OrbyError::IoError(e.to_string()))?;
        }
        Ok(())
    }

    async fn commit_vault_header(&self) -> Result<(), OrbyError> {
        let (vault_path, len, cursor) = {
            let store = self.inner.read();
            (store.vault_path.clone().unwrap(), store.len, store.cursor)
        };
        let header_path = vault_path.join("header.bin");
        let mut header_file = tokio::fs::OpenOptions::new()
            .write(true)
            .open(&header_path)
            .await
            .map_err(|e| OrbyError::IoError(e.to_string()))?;

        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        header_file
            .seek(std::io::SeekFrom::Start(24))
            .await
            .map_err(|e| OrbyError::IoError(e.to_string()))?;
        header_file
            .write_all(&(len as u64).to_le_bytes())
            .await
            .map_err(|e| OrbyError::IoError(e.to_string()))?;

        header_file
            .seek(std::io::SeekFrom::Start(32))
            .await
            .map_err(|e| OrbyError::IoError(e.to_string()))?;
        header_file
            .write_all(&(cursor as u64).to_le_bytes())
            .await
            .map_err(|e| OrbyError::IoError(e.to_string()))?;

        header_file
            .sync_all()
            .await
            .map_err(|e| OrbyError::IoError(e.to_string()))?;
        Ok(())
    }

    /// ストレージ上の Parallel Arrays を前方シフト（Compaction）します。
    pub(crate) async fn vault_delete_and_compact(&self, index: usize) -> Result<(), OrbyError> {
        let (vault_path, ring_buffer_lane_count, capacity) = {
            let store = self.inner.read();
            (
                store.vault_path.clone().unwrap(),
                store.ring_buffer_lane_count,
                store.capacity,
            )
        };

        if index >= capacity {
            return Ok(());
        }

        tokio::task::spawn_blocking(move || {
            let lane_files: Vec<std::fs::File> = (0..ring_buffer_lane_count)
                .map(|i| {
                    let p = vault_path.join(format!("lane_{}.bin", i));
                    OpenOptions::new()
                        .read(true)
                        .write(true)
                        .open(p)
                        .map_err(|e| OrbyError::IoError(e.to_string()))
                })
                .collect::<Result<Vec<_>, _>>()?;

            // Fast Move: index+1..len -> index
            if index < capacity - 1 {
                let buf_size = 64 * 1024; // 64KB block
                let mut buffer = vec![0u8; buf_size];

                for f in &lane_files {
                    let mut current_pos = index;
                    while current_pos < capacity - 1 {
                        let to_read = (buf_size / 16).min(capacity - 1 - current_pos);
                        if to_read == 0 {
                            break;
                        }

                        let read_offset = ((current_pos + 1) * 16) as u64;
                        let write_offset = (current_pos * 16) as u64;
                        let byte_count = to_read * 16;

                        f.read_at(&mut buffer[..byte_count], read_offset)
                            .map_err(|e| OrbyError::IoError(e.to_string()))?;
                        f.write_at(&buffer[..byte_count], write_offset)
                            .map_err(|e| OrbyError::IoError(e.to_string()))?;

                        current_pos += to_read;
                    }
                    // Zero out the last slot
                    let zeros = [0u8; 16];
                    f.write_at(&zeros, ((capacity - 1) * 16) as u64)
                        .map_err(|e| OrbyError::IoError(e.to_string()))?;
                    f.sync_all()
                        .map_err(|e| OrbyError::IoError(e.to_string()))?;
                }
            }
            Ok::<(), OrbyError>(())
        })
        .await
        .map_err(|e| OrbyError::IoError(e.to_string()))??;

        // Update Header
        self.commit_vault_header().await?;

        Ok(())
    }

    /// 特定の次元（レーン）の、特定のインデックスのみをストレージから抽出します。
    /// 戻り値は Vec<u128> だが、内部的にはスタックアロケーションに近い効率を目指す。
    pub fn load_to_stack_reactor(
        &self,
        lane_ids: &[usize],
        index: usize,
    ) -> Result<Vec<u128>, OrbyError> {
        let (vault_path, capacity) = {
            let store = self.inner.read();
            (
                store
                    .vault_path
                    .clone()
                    .ok_or(OrbyError::IoError("Vault not enabled".into()))?,
                store.capacity,
            )
        };

        if index >= capacity {
            return Err(OrbyError::IoError("Index out of bounds".into()));
        }

        let offset = (index * 16) as u64;

        let results: Vec<u128> = lane_ids
            .iter()
            .map(|&lane_id| {
                let p = vault_path.join(format!("lane_{}.bin", lane_id));
                let f = File::open(p).map_err(|e| OrbyError::IoError(e.to_string()))?;
                let mut buf = [0u8; 16];
                f.read_at(&mut buf, offset)
                    .map_err(|e| OrbyError::IoError(e.to_string()))?;
                Ok(u128::from_le_bytes(buf))
            })
            .collect::<Result<Vec<_>, OrbyError>>()?;

        Ok(results)
    }
}
