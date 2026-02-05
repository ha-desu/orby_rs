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
    /// Sync: メモリ + 物理ファイル同期
    /// Direct: ストレージのみ（省メモリ）
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

    pub async fn build(self) -> Result<Orby, OrbyError> {
        let engine = Orby::try_new(
            &self.ring_name,
            self.ring_buffer_lane_item_count,
            self.ring_buffer_lane_count,
            self.storage_mode.clone(),
            self.logic_mode,
            self.compaction,
            self.aof_enabled,
            self.capacity_usage_ratio,
        )
        .await?;

        if let Some(path) = self.restore_path {
            if !path.exists() {
                return Err(OrbyError::IoError(format!(
                    "Restore path not found at: {:?}",
                    path
                )));
            }

            let extension = path
                .extension()
                .and_then(|e| e.to_str())
                .map(|s| s.to_lowercase())
                .unwrap_or_default();

            match extension.as_str() {
                "aof" => {
                    engine.replay_aof(path).await?;
                }
                "orby" => {
                    engine.restore_from_storage(path).await?;
                }
                _ if path.is_dir() => {
                    // Assume Vault directory
                    engine.restore_from_vault().await?;
                }
                _ => {
                    return Err(OrbyError::IoError(format!(
                        "Unsupported restore format at: {:?}",
                        path
                    )));
                }
            }
        } else {
            // リストアが指定されていない場合
            match self.storage_mode {
                SaveMode::Vault(_) => {
                    // Vaultディレクトリが存在し、かつデータがある場合は自動ロードを試みる？
                    // ユーザー要件的には init_vault で空ファイルを作るか、既存があればロード。
                    // ここでは「クリーンな空のプール」として初期化するなら init_vault。
                    // 物理ファイルがある場合にエラーにするかロードするかはポリシー次第。
                    // init_vault は truncate(true) なので、新規作成。
                    engine.init_vault().await?;
                }
                SaveMode::MemoryOnly => {}
                _ => {
                    engine.init_storage_file().await?;
                }
            }
        }

        Ok(engine)
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
