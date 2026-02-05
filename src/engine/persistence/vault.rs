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
                .ok_or_else(|| OrbyError::Custom("Vault path is not set".into()))?;
            (p, store.capacity, store.ring_buffer_lane_count)
        };

        if !vault_path.exists() {
            tokio::fs::create_dir_all(&vault_path).await?;
        }

        // 1. Initialize Lane Files
        for i in 0..ring_buffer_lane_count {
            let lane_path = vault_path.join(format!("lane_{}.bin", i));
            let file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&lane_path)
                .await?;
            file.set_len((capacity * 16) as u64).await?;
            file.sync_all().await?;
        }

        // 2. Initialize Header File
        let header_path = vault_path.join("header.bin");
        let mut header_file = tokio::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&header_path)
            .await?;

        let mut header_data = vec![0u8; 4096];
        header_data[0..16].copy_from_slice(STORAGE_MAGIC_V1);
        header_data[16..24].copy_from_slice(&(capacity as u64).to_le_bytes());
        header_data[24..32].copy_from_slice(&(0u64).to_le_bytes()); // len
        header_data[32..40].copy_from_slice(&(0u64).to_le_bytes()); // cursor
        header_data[40..44].copy_from_slice(&(ring_buffer_lane_count as u32).to_le_bytes()); // lane_count

        use tokio::io::AsyncWriteExt;
        header_file.write_all(&header_data).await?;
        header_file.sync_all().await?;

        Ok(())
    }

    /// Vault からデータを復元します。
    pub(crate) async fn restore_from_vault(&self) -> Result<(), OrbyError> {
        let vault_path = {
            let store = self.inner.read();
            store
                .vault_path
                .clone()
                .ok_or_else(|| OrbyError::Custom("Vault path is not set".into()))?
        };

        let header_path = vault_path.join("header.bin");
        let mut header_file = tokio::fs::File::open(&header_path).await?;

        let mut header_data = [0u8; 4096];
        use tokio::io::AsyncReadExt;
        header_file.read_exact(&mut header_data).await?;

        if &header_data[0..16] != STORAGE_MAGIC_V1 {
            return Err(OrbyError::InvalidFormat(
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
                let metadata = std::fs::metadata(&lane_path)?;
                if metadata.len() != (v_cap * 16) as u64 {
                    return Err(OrbyError::Custom(format!(
                        "Lane {} size mismatch. Expected {}, found {}",
                        i,
                        v_cap * 16,
                        metadata.len()
                    )));
                }
            }

            store.len = v_len;
            store.cursor = v_cursor;

            // Load to memory
            if !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty() {
                for i in 0..v_dim {
                    let lane_path = vault_path.join(format!("lane_{}.bin", i));
                    let mut f = std::fs::File::open(&lane_path)?;

                    // Bulk read lane
                    let mut buf = vec![0u8; v_cap * 16];
                    use std::io::Read;
                    f.read_exact(&mut buf)?;

                    for (row, chunk) in buf.chunks_exact(16).enumerate() {
                        let val = u128::from_le_bytes(chunk.try_into().unwrap());
                        store.lanes[i].buffer[row] = crate::types::PulseCell::new(val);
                    }
                }
            }
        }

        Ok(())
    }

    /// メモリ上の全データをディスクに書き出し、完全に同期します。
    /// データの揮発を防ぐためのメンテナンスコマンドです。
    pub async fn sleep(&self) -> Result<(), OrbyError> {
        let inner = self.inner.clone();

        tokio::task::spawn_blocking(move || {
            let store = inner.read();
            let vault_path = store
                .vault_path
                .as_ref()
                .ok_or_else(|| OrbyError::Custom("Vault path is not set".into()))?;

            if !vault_path.exists() {
                std::fs::create_dir_all(vault_path)?;
            }

            use rayon::prelude::*;
            store
                .lanes
                .par_iter()
                .enumerate()
                .try_for_each(|(col, lane)| {
                    let p = vault_path.join(format!("lane_{}.bin", col));
                    let f = std::fs::OpenOptions::new()
                        .write(true)
                        .create(true)
                        .truncate(true)
                        .open(p)?;
                    let mut writer = std::io::BufWriter::new(f);

                    use std::io::Write;
                    for cell in &lane.buffer {
                        writer.write_all(&cell.as_u128().to_le_bytes())?;
                    }
                    writer.flush()?;
                    writer.get_ref().sync_all()?;

                    Ok::<(), OrbyError>(())
                })?;

            Ok::<(), OrbyError>(())
        })
        .await
        .map_err(|e| OrbyError::Custom(format!("Blocking task join error: {}", e)))??;

        // Header Update
        self.commit_vault_header().await?;

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

        let rows = std::sync::Arc::new(rows);
        let this = self.clone();

        tokio::task::spawn_blocking(move || {
            use rayon::prelude::*;

            (0..ring_buffer_lane_count)
                .into_par_iter()
                .try_for_each(|col| {
                    let p = vault_path.join(format!("lane_{}.bin", col));
                    let f = std::fs::OpenOptions::new().write(true).open(p)?;

                    let column_data: Vec<u128> = rows
                        .iter()
                        .map(|row| row.get(col).copied().unwrap_or(0))
                        .collect();

                    this.bulk_write_lane_file(&f, index, capacity, &column_data)?;
                    f.sync_all()?;

                    Ok::<(), OrbyError>(())
                })
        })
        .await
        .map_err(|e| OrbyError::Custom(format!("Blocking task join error: {}", e)))??;

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
                let f = std::fs::OpenOptions::new().write(true).open(p)?;

                this.bulk_write_lane_file(&f, start_index, capacity, &values)?;
                f.sync_all()?;
            }

            // 2. 他の全てのレーンに対して、同じインデックス範囲をゼロ埋め（垂直同期）
            let zeros = vec![0u128; values.len()];
            for col in 0..ring_buffer_lane_count {
                if col == lane_idx {
                    continue;
                }
                let p = vault_path.join(format!("lane_{}.bin", col));
                let f = std::fs::OpenOptions::new().write(true).open(p)?;

                this.bulk_write_lane_file(&f, start_index, capacity, &zeros)?;
                f.sync_all()?;
            }
            Ok::<(), OrbyError>(())
        })
        .await
        .map_err(|e| OrbyError::Custom(format!("Blocking task join error: {}", e)))??;

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
            f.write_at(&bytes, (start_idx * 16) as u64)?;
        } else {
            // ケースB: ラップアラウンドあり。2回に分割
            let len_to_end = capacity - start_idx;

            // 1. 末尾まで
            let bytes1: Vec<u8> = values[..len_to_end]
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect();
            f.write_at(&bytes1, (start_idx * 16) as u64)?;

            // 2. 先頭から
            let bytes2: Vec<u8> = values[len_to_end..]
                .iter()
                .flat_map(|v| v.to_le_bytes())
                .collect();
            f.write_at(&bytes2, 0)?;
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
            .await?;

        use tokio::io::{AsyncSeekExt, AsyncWriteExt};
        header_file.seek(std::io::SeekFrom::Start(24)).await?;
        header_file.write_all(&(len as u64).to_le_bytes()).await?;

        header_file.seek(std::io::SeekFrom::Start(32)).await?;
        header_file
            .write_all(&(cursor as u64).to_le_bytes())
            .await?;

        header_file.sync_all().await?;
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
                    OpenOptions::new().read(true).write(true).open(p)
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

                        f.read_at(&mut buffer[..byte_count], read_offset)?;
                        f.write_at(&buffer[..byte_count], write_offset)?;

                        current_pos += to_read;
                    }
                    // Zero out the last slot
                    let zeros = [0u8; 16];
                    f.write_at(&zeros, ((capacity - 1) * 16) as u64)?;
                    f.sync_all()?;
                }
            }
            Ok::<(), OrbyError>(())
        })
        .await
        .map_err(|e| OrbyError::Custom(format!("Blocking task join error: {}", e)))??;

        // Update Header
        self.commit_vault_header().await?;

        Ok(())
    }

    /// 既存の Vault データを検証し、メモリへ並列ロードします。
    pub(crate) async fn validate_and_load_vault(&self, strict: bool) -> Result<(), OrbyError> {
        let (vault_path, expected_cap, expected_dim) = {
            let store = self.inner.read();
            let p = store
                .vault_path
                .clone()
                .ok_or_else(|| OrbyError::Custom("Vault path is not set".into()))?;
            (p, store.capacity, store.ring_buffer_lane_count)
        };

        // 1. Metadata Validation (header.bin)
        let header_path = vault_path.join("header.bin");
        if !header_path.exists() {
            return Err(OrbyError::ConfigMismatch {
                name: self.name(),
                reason: "header.bin not found in existing vault".into(),
            });
        }

        let mut header_file = tokio::fs::File::open(&header_path).await?;

        let mut header_data = [0u8; 4096];
        use tokio::io::AsyncReadExt;
        header_file.read_exact(&mut header_data).await?;

        if &header_data[0..16] != STORAGE_MAGIC_V1 {
            return Err(OrbyError::ConfigMismatch {
                name: self.name(),
                reason: "Invalid magic number in existing vault".into(),
            });
        }

        let v_cap = u64::from_le_bytes(header_data[16..24].try_into().unwrap()) as usize;
        let v_len = u64::from_le_bytes(header_data[24..32].try_into().unwrap()) as usize;
        let v_cursor = u64::from_le_bytes(header_data[32..40].try_into().unwrap()) as usize;
        let v_dim = u32::from_le_bytes(header_data[40..44].try_into().unwrap()) as usize;

        if v_cap != expected_cap || v_dim != expected_dim {
            return Err(OrbyError::ConfigMismatch {
                name: self.name(),
                reason: format!(
                    "Vault configuration mismatch. Expected cap={}, dim={}, but found cap={}, dim={}",
                    expected_cap, expected_dim, v_cap, v_dim
                ),
            });
        }

        // 2. Physical Integrity Check
        if strict {
            for i in 0..v_dim {
                let lane_path = vault_path.join(format!("lane_{}.bin", i));
                let metadata =
                    std::fs::metadata(&lane_path).map_err(|e| OrbyError::ConfigMismatch {
                        name: self.name(),
                        reason: format!("Lane {} missing or inaccessible: {}", i, e),
                    })?;
                if metadata.len() != (v_cap * 16) as u64 {
                    return Err(OrbyError::ConfigMismatch {
                        name: self.name(),
                        reason: format!(
                            "Lane {} size mismatch. Expected {}, found {}",
                            i,
                            v_cap * 16,
                            metadata.len()
                        ),
                    });
                }
            }
        }

        // 3. Fast Deployment (Parallel Load)
        let mut join_handles = Vec::with_capacity(v_dim);
        for i in 0..v_dim {
            let lane_path = vault_path.join(format!("lane_{}.bin", i));
            let lane_idx = i;
            let cap = v_cap;

            let handle = tokio::task::spawn_blocking(move || {
                let mut f = std::fs::File::open(&lane_path)?;
                let mut buf = vec![0u8; cap * 16];
                use std::io::Read;
                f.read_exact(&mut buf)?;
                Ok::<(usize, Vec<u8>), OrbyError>((lane_idx, buf))
            });
            join_handles.push(handle);
        }

        {
            let mut store = self.inner.write();
            store.len = v_len;
            store.cursor = v_cursor;

            for handle in join_handles {
                let (lane_idx, buf) = handle
                    .await
                    .map_err(|e| OrbyError::Custom(format!("Blocking task join error: {}", e)))??;

                if !store.lanes.is_empty() && !store.lanes[lane_idx].buffer.is_empty() {
                    for (row, chunk) in buf.chunks_exact(16).enumerate() {
                        let val = u128::from_le_bytes(chunk.try_into().unwrap());
                        store.lanes[lane_idx].buffer[row] = crate::types::PulseCell::new(val);
                    }
                }
            }
        }

        Ok(())
    }
}
