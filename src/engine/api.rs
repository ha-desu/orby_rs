use crate::engine::iter::OrbyIterator;
use crate::engine::Orby;
use crate::error::OrbyError;
use crate::logic::ring;
use crate::row::PulseCellPack;
use crate::types::{LogicMode, PulseCell};
use std::collections::HashSet;
use std::sync::Arc;

impl Orby {
    /// 128-bit値のバッチを追加します。
    pub async fn insert_batch<I, T>(&self, items: I) -> Result<(), OrbyError>
    where
        I: IntoIterator<Item = T>,
        T: AsRef<[u128]>,
    {
        let raw_items: Vec<Vec<u128>> = items
            .into_iter()
            .map(|item| item.as_ref().to_vec())
            .collect();
        if raw_items.is_empty() {
            return Ok(());
        }

        let mut aof_data = Vec::new();
        let mut mirror_data = Vec::new();

        let (aof_sender, mirror_sender, has_vault, start_idx) = {
            let mut store = self.inner.write();
            let mode = store.logic_mode;
            let has_vault = store.vault_path.is_some();
            let start_idx = store.cursor;

            match mode {
                LogicMode::RingBuffer => {
                    ring::insert_batch(
                        &mut store,
                        raw_items.iter(),
                        &mut aof_data,
                        &mut mirror_data,
                    )?;
                }
            }
            (
                store.aof_sender.clone(),
                store.mirror_sender.clone(),
                has_vault,
                start_idx,
            )
        };

        if has_vault {
            self.commit_vault_batch(start_idx, raw_items).await?;
        }

        if let Some(sender) = aof_sender {
            if !aof_data.is_empty() {
                let _ = sender.send(aof_data).await;
            }
        }
        if let Some(sender) = mirror_sender {
            if !mirror_data.is_empty() {
                let _ = sender.send(mirror_data).await;
            }
        }
        Ok(())
    }

    /// 固定次元の行構造体を使用した高速なバッチ挿入を提供します。
    pub async fn insert_fixed<const N: usize>(
        &self,
        items: Vec<PulseCellPack<N>>,
    ) -> Result<(), OrbyError> {
        let mut aof_data = Vec::with_capacity(items.len() * N * 16);
        let mut mirror_data = Vec::new();

        // Convert to Vec<Vec<u128>> for Vault if needed
        let (aof_sender, mirror_sender, has_vault, start_idx) = {
            let mut store = self.inner.write();
            let has_vault = store.vault_path.is_some();
            let start_idx = store.cursor;

            ring::insert_fixed(&mut store, items.clone(), &mut aof_data, &mut mirror_data)?;

            (
                store.aof_sender.clone(),
                store.mirror_sender.clone(),
                has_vault,
                start_idx,
            )
        };

        if has_vault {
            let raw_rows: Vec<Vec<u128>> = items
                .iter()
                .map(|pack| pack.values.iter().map(|pc| pc.as_u128()).collect())
                .collect();
            self.commit_vault_batch(start_idx, raw_rows).await?;
        }

        if let Some(sender) = aof_sender {
            if !aof_data.is_empty() {
                let _ = sender.send(aof_data).await;
            }
        }
        if let Some(sender) = mirror_sender {
            if !mirror_data.is_empty() {
                let _ = sender.send(mirror_data).await;
            }
        }
        Ok(())
    }

    /// 条件に一致するデータを一件ずつ取得するためのイテレータを生成します。
    pub fn query_iter<'a, F>(&'a self, filter: F) -> OrbyIterator<'a, F>
    where
        F: Fn(&[PulseCell]) -> bool,
    {
        let store = self.inner.read();
        let logic_mode = store.logic_mode;
        let cursor = store.cursor;
        let cap = store.capacity;
        let len = store.len;

        let file = if store.lanes.is_empty() || store.lanes[0].buffer.is_empty() {
            store
                .mirror_path
                .as_ref()
                .and_then(|p| std::fs::File::open(p).ok())
        } else {
            None
        };

        OrbyIterator {
            store,
            filter,
            current_idx: 0,
            logic_mode,
            cursor,
            cap,
            len,
            file,
        }
    }

    /// カスタムフィルタ（クロージャ）を注入して並列スキャンを実行します。
    pub fn query_raw<F>(&self, filter: F, limit: usize) -> Vec<Arc<[u128]>>
    where
        F: Fn(&[PulseCell]) -> bool + Sync + Send,
    {
        let store = self.inner.read();
        match store.logic_mode {
            LogicMode::RingBuffer => ring::query_raw(&store, filter, limit),
        }
    }

    /// 特定のカラム（index）の値が `targets` のいずれかに一致するデータを最新順に検索します。
    pub fn find_by(&self, index: usize, targets: &HashSet<u128>, limit: usize) -> Vec<Arc<[u128]>> {
        if targets.is_empty() {
            return Vec::new();
        }
        self.query_raw(
            |row| {
                if let Some(val) = row.get(index) {
                    targets.contains(&val.as_u128())
                } else {
                    false
                }
            },
            limit,
        )
    }

    /// 特定のカラム（index）の値が `min` 以上 `max` 以下であるデータを最新順に検索します。
    pub fn find_custom(
        &self,
        index: usize,
        min: u128,
        max: u128,
        limit: usize,
    ) -> Vec<Arc<[u128]>> {
        self.query_raw(
            |row| {
                if let Some(val) = row.get(index) {
                    let v = val.as_u128();
                    v >= min && v <= max
                } else {
                    false
                }
            },
            limit,
        )
    }

    /// 特定の ID (u128) に一致するデータをプールから削除（ゼロ埋め）します。
    pub async fn purge_by_id(&self, index: usize, id: u128) {
        if id == 0 {
            return;
        }
        let mut aof_data = Vec::new();
        let mut mirror_data = Vec::new();

        let (aof_sender, mirror_sender) = {
            let mut store = self.inner.write();
            let logic_mode = store.logic_mode;
            match logic_mode {
                LogicMode::RingBuffer => {
                    ring::purge_by_id(&mut store, index, id, &mut aof_data, &mut mirror_data)
                }
            }
            (store.aof_sender.clone(), store.mirror_sender.clone())
        };
        if let Some(sender) = aof_sender {
            if !aof_data.is_empty() {
                let _ = sender.send(aof_data).await;
            }
        }
        if let Some(sender) = mirror_sender {
            if !mirror_data.is_empty() {
                let _ = sender.send(mirror_data).await;
            }
        }
    }

    /// 指定した ID を持つデータをその場で更新します。
    pub async fn update_by_id(&self, index: usize, id: u128, new_data: &[u128]) -> bool {
        let mut aof_data = Vec::new();
        let mut mirror_data = Vec::new();

        let (found, aof_sender, mirror_sender) = {
            let mut store = self.inner.write();
            let logic_mode = store.logic_mode;
            let found = match logic_mode {
                LogicMode::RingBuffer => ring::update_by_id(
                    &mut store,
                    index,
                    id,
                    new_data,
                    &mut aof_data,
                    &mut mirror_data,
                ),
            };
            (found, store.aof_sender.clone(), store.mirror_sender.clone())
        };
        if let Some(sender) = aof_sender {
            if !aof_data.is_empty() {
                let _ = sender.send(aof_data).await;
            }
        }
        if let Some(sender) = mirror_sender {
            if !mirror_data.is_empty() {
                let _ = sender.send(mirror_data).await;
            }
        }
        found
    }

    /// 特定のレーン（次元）に対して、複数のパルスを一括で流し込みます。
    /// 他のレーンは、整合性維持のため自動的にゼロクリアされます。
    pub async fn insert_lane_batch(
        &self,
        lane_idx: usize,
        values: &[u128],
    ) -> Result<(), OrbyError> {
        if values.is_empty() {
            return Ok(());
        }

        let mut aof_data = Vec::new();
        let mut mirror_data = Vec::new();

        let (aof_sender, mirror_sender, has_vault, start_idx) = {
            let mut store = self.inner.write();
            let start_idx = store.cursor;
            let has_vault = store.vault_path.is_some();

            ring::insert_lane_batch(
                &mut store,
                lane_idx,
                values,
                &mut aof_data,
                &mut mirror_data,
            )?;

            (
                store.aof_sender.clone(),
                store.mirror_sender.clone(),
                has_vault,
                start_idx,
            )
        };

        if has_vault {
            self.commit_vault_lane_batch(lane_idx, start_idx, values.to_vec())
                .await?;
        }

        if let Some(sender) = aof_sender {
            if !aof_data.is_empty() {
                let _ = sender.send(aof_data).await;
            }
        }
        if let Some(sender) = mirror_sender {
            if !mirror_data.is_empty() {
                let _ = sender.send(mirror_data).await;
            }
        }
        Ok(())
    }

    /// ID が存在すれば更新、なければ新規挿入します。
    pub async fn upsert(&self, index: usize, id: u128, data: &[u128]) -> Result<(), OrbyError> {
        let mut aof_data = Vec::new();
        let mut mirror_data = Vec::new();

        let (aof_sender, mirror_sender) = {
            let mut store = self.inner.write();
            let logic_mode = store.logic_mode;
            match logic_mode {
                LogicMode::RingBuffer => {
                    ring::upsert(&mut store, index, id, data, &mut aof_data, &mut mirror_data)?
                }
            }
            (store.aof_sender.clone(), store.mirror_sender.clone())
        };
        if let Some(sender) = aof_sender {
            if !aof_data.is_empty() {
                let _ = sender.send(aof_data).await;
            }
        }
        if let Some(sender) = mirror_sender {
            if !mirror_data.is_empty() {
                let _ = sender.send(mirror_data).await;
            }
        }
        Ok(())
    }

    /// 条件に一致する論理インデックスの一覧を返します。
    pub fn find_indices<F>(&self, filter: F, limit: usize) -> Vec<usize>
    where
        F: Fn(&[PulseCell]) -> bool + Sync + Send,
    {
        let store = self.inner.read();
        match store.logic_mode {
            LogicMode::RingBuffer => ring::find_indices(&store, filter, limit),
        }
    }

    pub fn get_at(&self, logical_index: usize) -> Option<Arc<[u128]>> {
        let store = self.inner.read();
        match store.logic_mode {
            LogicMode::RingBuffer => ring::get_at(&store, logical_index),
        }
    }

    /// 最新から指定件数分（limit）をガバッと取得します。
    pub fn take(&self, limit: usize) -> Vec<Arc<[u128]>> {
        self.query_raw(|_| true, limit)
    }

    /// ストアの内容をすべて破棄し、指定された新しいデータでメモリをリセットします。
    pub async fn purge_all_data<T>(&self, rows: Vec<T>) -> Result<(), OrbyError>
    where
        T: AsRef<[u128]>,
    {
        let mut aof_data = Vec::new();
        let mut mirror_data = Vec::new();

        let (aof_sender, mirror_sender) = {
            let mut store = self.inner.write();
            let logic_mode = store.logic_mode;

            match logic_mode {
                LogicMode::RingBuffer => ring::truncate(
                    &mut store,
                    rows.into_iter(),
                    &mut aof_data,
                    &mut mirror_data,
                )?,
            }

            (store.aof_sender.clone(), store.mirror_sender.clone())
        };

        if let Some(sender) = aof_sender {
            if !aof_data.is_empty() {
                let _ = sender.send(aof_data).await;
            }
        }
        if let Some(sender) = mirror_sender {
            if !mirror_data.is_empty() {
                let _ = sender.send(mirror_data).await;
            }
        }

        Ok(())
    }
}
