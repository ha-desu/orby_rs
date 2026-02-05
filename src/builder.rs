use crate::engine::Orby;
use crate::error::OrbyError;
use crate::types::{LogicMode, SaveMode};
use std::path::PathBuf;

/// `Orby` インスタンスを柔軟に構築するためのビルダーです。
pub struct OrbyBuilder {
    pub(crate) ring_name: String,
    pub(crate) ring_buffer_lane_item_count: usize,
    pub(crate) ring_buffer_lane_count: usize,
    pub(crate) storage_mode: SaveMode,
    pub(crate) logic_mode: LogicMode,
    pub(crate) compaction: bool,
    pub(crate) aof_enabled: bool,
    pub(crate) restore_path: Option<PathBuf>,
    pub(crate) capacity_usage_ratio: f64,
    pub(crate) autoload: bool,
    pub(crate) strict_check: bool,
    pub(crate) memory_limit: Option<u64>, // bytes
}

impl OrbyBuilder {
    pub fn new(ring_name: &str) -> Self {
        Self {
            ring_name: ring_name.to_string(),
            ring_buffer_lane_item_count: 10_000,
            ring_buffer_lane_count: 2,
            storage_mode: SaveMode::MemoryOnly,
            logic_mode: LogicMode::RingBuffer,
            compaction: false,
            aof_enabled: false,
            restore_path: None,
            capacity_usage_ratio: 0.8,
            autoload: true,
            strict_check: true,
            memory_limit: None,
        }
    }

    /// RingBufferのLane数を設定します。
    /// DBのレコードのフィールド数のような概念です
    pub fn ring_buffer_lane_count(mut self, count: usize) -> Self {
        self.ring_buffer_lane_count = count;
        self
    }

    /// RingBufferのLaneに載せるアイテムの最大数を設定します。
    /// DBのレコードのような概念ですが1次元のデータリストです
    pub fn ring_buffer_lane_item_count(mut self, slots: usize) -> Self {
        self.ring_buffer_lane_item_count = slots;
        self
    }

    /// ストレージの保存モードを設定します。
    /// MemoryOnly: メモリのみ
    /// Vault: マルチレーンの特殊永続化
    pub fn with_storage(mut self, mode: SaveMode) -> Self {
        self.storage_mode = mode;
        self
    }

    /// Orby の物理モードを設定します。
    /// LogicMode::RingBuffer: 時系列ログ用（古いデータを自動上書き）
    pub fn logic_mode(mut self, mode: LogicMode) -> Self {
        self.logic_mode = mode;
        self
    }

    /// 削除時のコンパクション（詰め）動作を設定します。
    /// true: 削除時にデータをスライドして詰める (Packed Mode)
    /// false: 削除箇所をゼロ埋めするだけ (Sparse Mode / Ring)
    pub fn compaction(mut self, enabled: bool) -> Self {
        self.compaction = enabled;
        self
    }

    /// AOF (Append Only File) ログを有効にします。
    pub fn enable_aof(mut self, enabled: bool) -> Self {
        self.aof_enabled = enabled;
        self
    }

    /// 復元元のファイルを指定します。拡張子が .aof ならリプレイ、.orby ならスナップショットとして扱います。
    pub fn from_file<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.restore_path = Some(path.into());
        self
    }

    /// メモリ使用率の安全限界を設定します (0.0 ~ 1.0)。
    /// 1.0 を指定するとチェックを実質的に無効化できます。
    pub fn capacity_usage_ratio(mut self, ratio: f64) -> Self {
        self.capacity_usage_ratio = ratio.clamp(0.0, 1.0);
        self
    }

    /// 既存データがある場合に自動でロードするかを設定します。
    pub fn autoload(mut self, enabled: bool) -> Self {
        self.autoload = enabled;
        self
    }

    /// ロード時に厳格な整合性チェックを行うかを設定します。
    pub fn strict_check(mut self, enabled: bool) -> Self {
        self.strict_check = enabled;
        self
    }

    /// メモリ使用量の上限（バイト単位）を設定します。
    pub fn memory_limit(mut self, limit_bytes: u64) -> Self {
        self.memory_limit = Some(limit_bytes);
        self
    }

    pub async fn build(self) -> Result<Orby, OrbyError> {
        // 1. Memory Safety Guard
        self.check_memory_safety()?;

        let ring_name = self.ring_name.clone();
        let storage_mode = self.storage_mode.clone();
        let autoload = self.autoload;
        let strict_check = self.strict_check;

        let restore_path = self.restore_path.clone();

        let engine = Orby::try_new(
            &ring_name,
            self.ring_buffer_lane_item_count,
            self.ring_buffer_lane_count,
            storage_mode.clone(),
            self.logic_mode,
            self.compaction,
            self.aof_enabled,
            self.capacity_usage_ratio,
        )
        .await?;

        if let Some(path) = restore_path {
            OrbyBuilder::handle_restore_static(&engine, path).await?;
        } else {
            // 自動ロードの試行
            match &storage_mode {
                SaveMode::Vault(opt_path) => {
                    let base_path = opt_path
                        .as_ref()
                        .cloned()
                        .unwrap_or_else(|| PathBuf::from("db_data"));
                    let vault_dir = base_path.join(&ring_name);

                    if vault_dir.exists() && autoload {
                        // 既存データの検出とロード
                        engine.validate_and_load_vault(strict_check).await?;
                    } else {
                        // 新規作成
                        engine.init_vault().await?;
                    }
                }
                SaveMode::MemoryOnly => {}
            }
        }

        Ok(engine)
    }

    fn check_memory_safety(&self) -> Result<(), OrbyError> {
        let required_bytes =
            (self.ring_buffer_lane_count as u64) * (self.ring_buffer_lane_item_count as u64) * 16;

        let limit = if let Some(l) = self.memory_limit {
            l
        } else {
            use sysinfo::System;
            let mut sys = System::new();
            sys.refresh_memory();
            let available = sys.available_memory();
            if available == 0 {
                // If sysinfo fails, assume 1GB for safety in test environments
                1024 * 1024 * 1024
            } else {
                available
            }
        };

        if required_bytes > limit {
            return Err(OrbyError::InsufficientMemory {
                requested_mb: required_bytes / 1024 / 1024,
                available_mb: limit / 1024 / 1024,
            });
        }
        Ok(())
    }

    async fn handle_restore_static(engine: &Orby, path: PathBuf) -> Result<(), OrbyError> {
        if !path.exists() {
            return Err(OrbyError::IoError(format!(
                "Restore path not found: {:?}",
                path
            )));
        }

        let extension = path
            .extension()
            .and_then(|e| e.to_str())
            .map(|s| s.to_lowercase())
            .unwrap_or_default();
        match extension.as_str() {
            "aof" => engine.replay_aof(path).await?,
            "orby" => engine.restore_from_storage(path).await?,
            _ if path.is_dir() => engine.restore_from_vault().await?,
            _ => {
                return Err(OrbyError::IoError(format!(
                    "Unsupported restore format: {:?}",
                    path
                )))
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_builder() {
        let label = "test_builder";
        let engine = Orby::builder(label)
            .ring_buffer_lane_item_count(10)
            .ring_buffer_lane_count(3)
            .compaction(true)
            .build()
            .await
            .unwrap();

        assert_eq!(engine.name(), label);
        assert_eq!(engine.len(), 0);
    }
}
