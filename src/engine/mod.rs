pub mod api;
pub mod iter;
pub mod persistence;

pub use iter::OrbyIterator;

#[cfg(test)]
mod tests;

use crate::builder::OrbyBuilder;
use crate::error::OrbyError;
use crate::logic::OrbyRingBufferSilo;
use crate::types::{LogicMode, SaveMode};
use parking_lot::RwLock;
use std::sync::Arc;
use sysinfo::System;

/// `Orby` は、固定長128-bitデータの等価比較と時系列スキャンに特化した高速なインメモリ・インデックスエンジンです。
#[derive(Clone)]
pub struct Orby {
    pub(crate) inner: Arc<RwLock<OrbyRingBufferSilo>>,
}

impl Orby {
    /// 新しい Builder を生成します。
    pub fn builder(label: &str) -> OrbyBuilder {
        OrbyBuilder::new(label)
    }

    /// 主要な設定を引数に指定して、新しい `Orby` インスタンスを初期化します。
    pub async fn new(
        label: &str,
        ring_buffer_lane_item_count: usize,
        ring_buffer_lane_count: usize,
        storage_mode: SaveMode,
        logic_mode: LogicMode,
    ) -> Result<Self, OrbyError> {
        Self::builder(label)
            .ring_buffer_lane_item_count(ring_buffer_lane_item_count)
            .ring_buffer_lane_count(ring_buffer_lane_count)
            .with_storage(storage_mode)
            .logic_mode(logic_mode)
            .build()
            .await
    }

    /// ストアのメタ情報を取得します。
    /// 返り値: (現在のデータ件数, 最大キャパシティ, レーン数)
    pub fn meta(&self) -> (usize, usize, usize) {
        let store = self.inner.read();
        (store.len, store.capacity, store.ring_buffer_lane_count)
    }

    pub(crate) async fn try_new(
        name: &str,
        capacity: usize,
        ring_buffer_lane_count: usize,
        storage_mode: SaveMode,
        logic_mode: LogicMode,
        compaction: bool,
        aof_enabled: bool,
        capacity_usage_ratio: f64,
    ) -> Result<Self, OrbyError> {
        let row_bytes = ring_buffer_lane_count * 16;
        let required_bytes = if matches!(storage_mode, SaveMode::Direct(_)) {
            0
        } else {
            capacity as u64 * row_bytes as u64
        };

        if required_bytes > 0 {
            let mut sys = System::new_all();
            sys.refresh_memory();
            let available_bytes = sys.available_memory();
            let safety_limit = (available_bytes as f64 * capacity_usage_ratio) as u64;

            if safety_limit > 0 && required_bytes > safety_limit {
                return Err(OrbyError::InsufficientMemory {
                    requested_mb: required_bytes / 1024 / 1024,
                    available_mb: available_bytes / 1024 / 1024,
                });
            }
        }

        let mut aof_sender = None;
        let mut mirror_sender = None;
        let mut mirror_path_buf = None;
        let mut vault_path_buf = None;

        let (resolved_mirror_path, resolved_aof_path) = match &storage_mode {
            SaveMode::Vault(opt_path) => {
                let base_path = opt_path
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| std::path::PathBuf::from("db_data"));
                let vault_dir = base_path.join(name);
                vault_path_buf = Some(vault_dir);
                (None, None)
            }
            SaveMode::Direct(opt_path) => {
                let base_path = opt_path
                    .as_ref()
                    .cloned()
                    .unwrap_or_else(|| std::path::PathBuf::from("."));

                let (orby_p, aof_p) = if base_path.is_dir() {
                    (
                        base_path.join(format!("{}.orby", name)),
                        base_path.join(format!("{}.aof", name)),
                    )
                } else {
                    let aof = base_path.with_extension("aof");
                    (base_path, aof)
                };

                // 親ディレクトリが存在しない場合は作成する
                if let Some(parent) = orby_p.parent() {
                    if !parent.as_os_str().is_empty() {
                        let _ = std::fs::create_dir_all(parent);
                    }
                }

                (Some(orby_p), Some(aof_p))
            }
            _ => (None, None),
        };

        if mirror_path_buf.is_none() {
            mirror_path_buf = resolved_mirror_path.clone();
        }

        if aof_enabled {
            let aof_path = resolved_aof_path
                .clone()
                .unwrap_or_else(|| std::path::PathBuf::from(format!("{}.aof", name)));
            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);
            Self::spawn_aof_worker(name.to_string(), aof_path, rx);
            aof_sender = Some(tx);
        }

        if matches!(storage_mode, SaveMode::Direct(_)) {
            let path = mirror_path_buf.clone().unwrap();
            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<(u64, Vec<u8>)>>(1024);
            Self::spawn_mirror_worker(name.to_string(), path, rx);
            mirror_sender = Some(tx);
        }

        use crate::logic::OrbyRingBuffer;
        let lanes = if matches!(storage_mode, SaveMode::Direct(_)) {
            (0..ring_buffer_lane_count)
                .map(|_| OrbyRingBuffer { buffer: Vec::new() })
                .collect()
        } else {
            (0..ring_buffer_lane_count)
                .map(|_| OrbyRingBuffer::new(capacity))
                .collect()
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(OrbyRingBufferSilo {
                name: name.to_string(),
                lanes,
                cursor: 0,
                len: 0,
                capacity,
                ring_buffer_lane_count,
                compaction,
                logic_mode,
                storage_mode,
                aof_sender,
                mirror_sender,
                mirror_path: mirror_path_buf,
                vault_path: vault_path_buf,
            })),
        })
    }

    pub fn name(&self) -> String {
        self.inner.read().name.clone()
    }
    pub fn len(&self) -> usize {
        self.inner.read().len
    }
    pub fn count_active(&self) -> usize {
        let store = self.inner.read();
        crate::logic::ring::count_active(&store)
    }
    pub fn is_empty(&self) -> bool {
        self.inner.read().len == 0
    }
    pub async fn delete(&self, index: usize) -> bool {
        let (res, has_vault, compaction) = {
            let mut store = self.inner.write();
            let (mut aof_data, mut mirror_data) = (Vec::new(), Vec::new());
            let res =
                crate::logic::ring::delete(&mut *store, index, &mut aof_data, &mut mirror_data);
            let has_vault = store.vault_path.is_some();
            let compaction = store.compaction;
            self.dispatch_persistence(&mut *store, aof_data, mirror_data);
            (res, has_vault, compaction)
        };

        if res && has_vault {
            if compaction {
                let _ = self.vault_delete_and_compact(index).await;
            } else {
                // Sparse delete: zero out entry in Vault
                let dim = self.meta().2;
                let zeros = vec![vec![0u128; dim]];
                let _ = self.commit_vault_batch(index, zeros).await;
            }
        }
        res
    }

    fn dispatch_persistence(
        &self,
        store: &mut OrbyRingBufferSilo,
        aof_data: Vec<u8>,
        mirror_data: Vec<(u64, Vec<u8>)>,
    ) {
        if let Some(sender) = &store.aof_sender {
            if !aof_data.is_empty() {
                let _ = sender.try_send(aof_data);
            }
        }
        if let Some(sender) = &store.mirror_sender {
            if !mirror_data.is_empty() {
                let _ = sender.try_send(mirror_data);
            }
        }
    }
}
