use crate::engine::Orby;
use crate::error::OrbyError;
use crate::types::{LogicMode, SaveMode};
use std::path::PathBuf;

/// `Orby` インスタンスを柔軟に構築するためのビルダーです。
pub struct OrbyBuilder {
    pub(crate) name: String,
    pub(crate) capacity: usize,
    pub(crate) dimension: usize,
    pub(crate) storage_mode: SaveMode,
    pub(crate) logic_mode: LogicMode,
    pub(crate) aof_enabled: bool,
    pub(crate) restore_path: Option<PathBuf>,
    pub(crate) capacity_usage_ratio: f64,
}

impl OrbyBuilder {
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            capacity: 10_000,
            dimension: 2,
            storage_mode: SaveMode::MemoryOnly,
            logic_mode: LogicMode::Ring,
            aof_enabled: false,
            restore_path: None,
            capacity_usage_ratio: 0.8,
        }
    }

    /// インデックスの最大収容行数を設定します。
    /// DBの最大レコード数に相当します。
    pub fn capacity(mut self, cap: usize) -> Self {
        self.capacity = cap;
        self
    }

    /// 1行あたりのデータ要素数（カラム数）を設定します。
    /// DBのレコードのフィールド数に相当します。
    pub fn dimension(mut self, dim: usize) -> Self {
        self.dimension = dim;
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
    /// LogicMode::Ring: 時系列ログ用（古いデータを自動上書き）
    pub fn logic_mode(mut self, mode: LogicMode) -> Self {
        self.logic_mode = mode;
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
        let is_memory_mode = matches!(self.storage_mode, SaveMode::MemoryOnly);
        let engine = Orby::try_new(
            &self.name,
            self.capacity,
            self.dimension,
            self.storage_mode,
            self.logic_mode,
            self.aof_enabled,
            self.capacity_usage_ratio,
        )
        .await?;

        if let Some(path) = self.restore_path {
            if !path.exists() {
                return Err(OrbyError::IoError(format!(
                    "Restore file not found at: {:?}",
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
                _ => {
                    return Err(OrbyError::IoError(format!(
                        "Unsupported restore file extension: '.{}'. Expected '.orby' or '.aof'",
                        extension
                    )));
                }
            }
        } else {
            // リストアが指定されていない場合、クリーンな空のプールとして初期化
            // (メモリモード以外の場合はストレージファイルを準備)
            if !is_memory_mode {
                engine.init_storage_file().await?;
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
            .capacity(20)
            .dimension(3)
            .build()
            .await
            .unwrap();

        assert_eq!(engine.name(), label);
        assert_eq!(engine.len(), 0);
    }
}
