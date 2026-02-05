use crate::engine::Orby;
use crate::error::OrbyError;
use std::path::PathBuf;
use tokio::io::AsyncReadExt;

impl Orby {
    pub(crate) fn spawn_aof_worker(
        name: String,
        path: PathBuf,
        mut rx: tokio::sync::mpsc::Receiver<Vec<u8>>,
    ) {
        tokio::spawn(async move {
            use tokio::io::AsyncWriteExt;
            let mut file_handle = None;

            while let Some(data) = rx.recv().await {
                if file_handle.is_none() {
                    let file_res = tokio::fs::OpenOptions::new()
                        .create(true)
                        .append(true)
                        .open(&path)
                        .await;
                    match file_res {
                        Ok(f) => file_handle = Some(f),
                        Err(e) => {
                            eprintln!("[Orby:{}] Failed to open AOF file {:?}: {}", name, path, e);
                            continue;
                        }
                    }
                }

                if let Some(ref mut file) = file_handle {
                    if let Err(e) = file.write_all(&data).await {
                        eprintln!("[Orby:{}] AOF write failed: {}", name, e);
                        break;
                    }
                    let _ = file.flush().await;
                }
            }
        });
    }

    pub(crate) async fn replay_aof(&self, path: PathBuf) -> Result<(), OrbyError> {
        let mut file = tokio::fs::File::open(&path).await?;
        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).await?;

        let mut pos = 0;
        let dim = self.meta().2;

        while pos < buffer.len() {
            let op = buffer[pos];
            pos += 1;

            match op {
                crate::logic::AOF_OP_INSERT => {
                    let mut row = Vec::with_capacity(dim);
                    for _ in 0..dim {
                        let val = u128::from_le_bytes(buffer[pos..pos + 16].try_into().unwrap());
                        row.push(val);
                        pos += 16;
                    }
                    self.insert_batch(vec![row]).await?;
                }
                crate::logic::AOF_OP_PURGE => {
                    let index =
                        u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    let id = u128::from_le_bytes(buffer[pos..pos + 16].try_into().unwrap());
                    pos += 16;
                    self.purge_by_id(index, id).await;
                }
                crate::logic::AOF_OP_UPDATE => {
                    let index =
                        u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    let id = u128::from_le_bytes(buffer[pos..pos + 16].try_into().unwrap());
                    pos += 16;
                    let mut new_data = Vec::with_capacity(dim);
                    for _ in 0..dim {
                        let val = u128::from_le_bytes(buffer[pos..pos + 16].try_into().unwrap());
                        new_data.push(val);
                        pos += 16;
                    }
                    self.update_by_id(index, id, &new_data).await;
                }
                crate::logic::AOF_OP_TRUNCATE => {
                    self.purge_all_data(Vec::<[u128; 1]>::new()).await?;
                }
                crate::logic::AOF_OP_LANE_BATCH => {
                    let lane_idx =
                        u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    let count =
                        u32::from_le_bytes(buffer[pos..pos + 4].try_into().unwrap()) as usize;
                    pos += 4;
                    let mut values = Vec::with_capacity(count);
                    for _ in 0..count {
                        let val = u128::from_le_bytes(buffer[pos..pos + 16].try_into().unwrap());
                        values.push(val);
                        pos += 16;
                    }
                    self.insert_lane_batch(lane_idx, &values).await?;
                }
                _ => break,
            }
        }
        Ok(())
    }
}
