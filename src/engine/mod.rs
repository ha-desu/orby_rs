pub mod api;
pub mod iter;
pub mod persistence;

pub use iter::OrbyIterator;

#[cfg(test)]
mod tests;

use crate::builder::OrbyBuilder;
use crate::error::OrbyError;
use crate::logic::OrbyStore;
use crate::types::{LogicMode, PulseCell, SaveMode};
use parking_lot::RwLock;
use std::sync::Arc;
use sysinfo::System;

/// `Orby` は、固定長128-bitデータの等価比較と時系列スキャンに特化した高速なインメモリ・インデックスエンジンです。
#[derive(Clone)]
pub struct Orby {
    pub(crate) inner: Arc<RwLock<OrbyStore>>,
}

impl Orby {
    /// 新しい Builder を生成します。
    pub fn builder(label: &str) -> OrbyBuilder {
        OrbyBuilder::new(label)
    }

    /// 主要な設定を引数に指定して、新しい `Orby` インスタンスを初期化します。
    pub async fn new(
        label: &str,
        capacity: usize,
        dimension: usize,
        storage_mode: SaveMode,
        logic_mode: LogicMode,
    ) -> Result<Self, OrbyError> {
        Self::builder(label)
            .capacity(capacity)
            .dimension(dimension)
            .with_storage(storage_mode)
            .logic_mode(logic_mode)
            .build()
            .await
    }

    /// ストアのメタ情報を取得します。
    /// 返り値: (現在のデータ件数, 最大キャパシティ, 次元数)
    pub fn meta(&self) -> (usize, usize, usize) {
        let store = self.inner.read();
        (store.len, store.capacity, store.dimension)
    }

    pub(crate) async fn try_new(
        name: &str,
        capacity: usize,
        dimension: usize,
        storage_mode: SaveMode,
        logic_mode: LogicMode,
        aof_enabled: bool,
        capacity_usage_ratio: f64,
    ) -> Result<Self, OrbyError> {
        let row_bytes = (dimension * 16 + 63) & !63;
        let stride = row_bytes / 16;

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
        let mirror_path_buf: Option<std::path::PathBuf>;

        let (resolved_mirror_path, resolved_aof_path) = match &storage_mode {
            SaveMode::Sync(opt_path) | SaveMode::Direct(opt_path) => {
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

        mirror_path_buf = resolved_mirror_path.clone();

        if aof_enabled {
            let aof_path = resolved_aof_path
                .clone()
                .unwrap_or_else(|| std::path::PathBuf::from(format!("{}.aof", name)));
            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<u8>>(1024);
            Self::spawn_aof_worker(name.to_string(), aof_path, rx);
            aof_sender = Some(tx);
        }

        if matches!(storage_mode, SaveMode::Sync(_) | SaveMode::Direct(_)) {
            let path = mirror_path_buf.clone().unwrap();
            let (tx, rx) = tokio::sync::mpsc::channel::<Vec<(u64, Vec<u8>)>>(1024);
            Self::spawn_mirror_worker(name.to_string(), path, rx);
            mirror_sender = Some(tx);
        }

        let buffer = if matches!(storage_mode, SaveMode::Direct(_)) {
            Vec::new()
        } else {
            vec![PulseCell::new(0); capacity * stride]
        };

        Ok(Self {
            inner: Arc::new(RwLock::new(OrbyStore {
                name: name.to_string(),
                buffer,
                head: 0,
                len: 0,
                capacity,
                dimension,
                stride,
                logic_mode,
                storage_mode,
                aof_sender,
                mirror_sender,
                mirror_path: mirror_path_buf,
            })),
        })
    }

    pub fn name(&self) -> String {
        self.inner.read().name.clone()
    }
    pub fn len(&self) -> usize {
        self.inner.read().len
    }
    pub fn is_empty(&self) -> bool {
        self.inner.read().len == 0
    }
}
