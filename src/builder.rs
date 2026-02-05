use crate::engine::Orby;
use crate::error::OrbyError;
use crate::types::{LogicMode, SaveMode};
use std::path::PathBuf;

/// Builder for creating flexible `Orby` instances.
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
            storage_mode: SaveMode::Vault(None),
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

    /// Sets the number of lanes (dimensions) in the ring buffer.
    /// This is conceptually similar to the number of fields in a database record.
    pub fn ring_buffer_lane_count(mut self, count: usize) -> Self {
        self.ring_buffer_lane_count = count;
        self
    }

    /// Sets the maximum number of items (capacity) for each lane in the ring buffer.
    pub fn ring_buffer_lane_item_count(mut self, slots: usize) -> Self {
        self.ring_buffer_lane_item_count = slots;
        self
    }

    /// Sets the storage persistence mode.
    /// - `MemoryOnly`: In-memory only.
    /// - `Vault`: Specialized multi-lane persistence.
    pub fn with_storage(mut self, mode: SaveMode) -> Self {
        self.storage_mode = mode;
        self
    }

    /// Sets the logic mode of Orby.
    /// - `LogicMode::RingBuffer`: For time-series logs (automatically overwrites old data).
    pub fn logic_mode(mut self, mode: LogicMode) -> Self {
        self.logic_mode = mode;
        self
    }

    /// Sets the compaction behavior upon deletion.
    /// - `true`: Slide data to fill gaps (Packed Mode).
    /// - `false`: Zero out the deleted slot (Sparse Mode / Ring).
    pub fn compaction(mut self, enabled: bool) -> Self {
        self.compaction = enabled;
        self
    }

    /// Enables AOF (Append Only File) logging.
    pub fn enable_aof(mut self, enabled: bool) -> Self {
        self.aof_enabled = enabled;
        self
    }

    /// Specifies the source file for restoration.
    /// If extension is `.aof`, it replays; if `.orby` (or directory for Vault), it treats as a snapshot/vault.
    pub fn from_file<P: Into<PathBuf>>(mut self, path: P) -> Self {
        self.restore_path = Some(path.into());
        self
    }

    /// Sets the safety limit for memory usage ratio (0.0 ~ 1.0).
    /// Specifying 1.0 effectively disables the check.
    pub fn capacity_usage_ratio(mut self, ratio: f64) -> Self {
        self.capacity_usage_ratio = ratio.clamp(0.0, 1.0);
        self
    }

    /// Configures whether to automatically load existing data if available.
    pub fn autoload(mut self, enabled: bool) -> Self {
        self.autoload = enabled;
        self
    }

    /// Configures whether to perform strict consistency checks during loading.
    pub fn strict_check(mut self, enabled: bool) -> Self {
        self.strict_check = enabled;
        self
    }

    /// Sets the memory usage limit in bytes.
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
            return Err(OrbyError::Custom(format!(
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
            _ if path.is_dir() => engine.restore_from_vault().await?,
            _ => {
                return Err(OrbyError::Custom(format!(
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
