use crate::engine::Orby;
use crate::error::OrbyError;
use crate::types::{PulseCell, STORAGE_MAGIC_V1};
use std::io::Write;
use std::path::{Path, PathBuf};
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
        let l_dim = u32::from_le_bytes(header[24..28].try_into().unwrap()) as usize;
        let l_pdim = u32::from_le_bytes(header[28..32].try_into().unwrap()) as usize;
        let l_len = u64::from_le_bytes(header[40..48].try_into().unwrap()) as usize;
        let l_head = u64::from_le_bytes(header[48..56].try_into().unwrap()) as usize;

        {
            let mut store = self.inner.write();
            if l_cap != store.capacity || l_dim != store.dimension || l_pdim != store.stride {
                return Err(OrbyError::DimensionMismatch {
                    pool_name: store.name.clone(),
                    expected: store.dimension,
                    found: l_dim,
                });
            }

            store.len = l_len;
            store.head = l_head;

            if !store.buffer.is_empty() {
                let data_size = store.capacity * store.stride * 16;
                let mut data_buf = vec![0u8; data_size];
                file.read_exact(&mut data_buf)
                    .await
                    .map_err(|e| OrbyError::IoError(e.to_string()))?;

                for (i, chunk) in data_buf.chunks_exact(16).enumerate() {
                    let val = u128::from_le_bytes(chunk.try_into().unwrap());
                    store.buffer[i] = PulseCell::new(val);
                }
            }
        }

        Ok(())
    }

    /// ストレージファイルを新規作成または初期化し、ヘッダーを書き込みます。
    pub(crate) async fn init_storage_file(&self) -> Result<(), OrbyError> {
        let (path_opt, cap, dim, stride, logic) = {
            let store = self.inner.read();
            (
                store.mirror_path.clone(),
                store.capacity,
                store.dimension,
                store.stride,
                store.logic_mode,
            )
        };

        if let Some(path) = path_opt {
            use tokio::io::AsyncWriteExt;
            let mut file = tokio::fs::OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .await
                .map_err(|e| OrbyError::IoError(format!("Failed to create storage file: {}", e)))?;

            let data_size = (cap * stride * 16) as u64;
            file.set_len(crate::types::HEADER_SIZE + data_size)
                .await
                .map_err(|e| OrbyError::IoError(e.to_string()))?;

            let mut header = vec![0u8; crate::types::HEADER_SIZE as usize];
            header[0..16].copy_from_slice(STORAGE_MAGIC_V1);
            header[16..24].copy_from_slice(&(cap as u64).to_le_bytes());
            header[24..28].copy_from_slice(&(dim as u32).to_le_bytes());
            header[28..32].copy_from_slice(&(stride as u32).to_le_bytes());
            header[32] = logic.as_u8();

            file.write_all(&header)
                .await
                .map_err(|e| OrbyError::IoError(e.to_string()))?;
            file.flush()
                .await
                .map_err(|e| OrbyError::IoError(e.to_string()))?;
        }
        Ok(())
    }

    pub(crate) fn spawn_mirror_worker(
        name: String,
        path: PathBuf,
        mut rx: tokio::sync::mpsc::Receiver<Vec<(u64, Vec<u8>)>>,
    ) {
        tokio::spawn(async move {
            use tokio::io::{AsyncSeekExt, AsyncWriteExt};

            let mut file_handle = None;
            while let Some(ops) = rx.recv().await {
                if file_handle.is_none() {
                    let file_res = tokio::fs::OpenOptions::new().write(true).open(&path).await;
                    match file_res {
                        Ok(f) => file_handle = Some(f),
                        Err(e) => {
                            eprintln!(
                                "[Orby:{}] Failed to open Mirror file {:?}: {}",
                                name, path, e
                            );
                            continue;
                        }
                    }
                }

                if let Some(ref mut file) = file_handle {
                    for (offset, data) in ops {
                        if let Err(e) = file.seek(std::io::SeekFrom::Start(offset)).await {
                            eprintln!("[Orby:{}] Mirror seek failed: {}", name, e);
                            continue;
                        }
                        if let Err(e) = file.write_all(&data).await {
                            eprintln!("[Orby:{}] Mirror write failed: {}", name, e);
                        }
                    }
                    let _ = file.flush().await;
                }
            }
        });
    }

    /// 指定されたファイルパスに 4KB ヘッダー付きのスナップショットを保存します。
    pub fn write_snapshot_to_file(&self, path: &Path) -> Result<(), String> {
        let store = self.inner.read();

        let file = std::fs::File::create(path).map_err(|e| e.to_string())?;
        let mut writer = std::io::BufWriter::new(file);

        let mut header = vec![0u8; crate::types::HEADER_SIZE as usize];
        header[0..16].copy_from_slice(STORAGE_MAGIC_V1);
        header[16..24].copy_from_slice(&(store.capacity as u64).to_le_bytes());
        header[24..28].copy_from_slice(&(store.dimension as u32).to_le_bytes());
        header[28..32].copy_from_slice(&(store.stride as u32).to_le_bytes());
        header[32] = store.logic_mode.as_u8();
        header[40..48].copy_from_slice(&(store.len as u64).to_le_bytes());
        header[48..56].copy_from_slice(&(store.head as u64).to_le_bytes());

        let name_bytes = store.name.as_bytes();
        let name_len = name_bytes.len().min(64);
        header[64..64 + name_len].copy_from_slice(&name_bytes[..name_len]);

        writer.write_all(&header).map_err(|e| e.to_string())?;

        if !store.buffer.is_empty() {
            for field in &store.buffer {
                writer
                    .write_all(&field.as_u128().to_le_bytes())
                    .map_err(|e| e.to_string())?;
            }
        }

        writer.flush().map_err(|e| e.to_string())?;
        writer.get_mut().sync_all().map_err(|e| e.to_string())?;
        Ok(())
    }
}
