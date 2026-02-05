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
        let iter = items.into_iter();
        let (lower, _) = iter.size_hint();
        let mut aof_data = Vec::new();
        let mut mirror_data = Vec::new();

        let (aof_sender, mirror_sender) = {
            let mut store = self.inner.write();
            let logic_mode = store.logic_mode;
            let has_aof = store.aof_sender.is_some();

            if has_aof {
                aof_data.reserve(lower.max(1) * store.dimension * 16);
            }

            match logic_mode {
                LogicMode::Ring => {
                    ring::insert_batch(&mut store, iter, &mut aof_data, &mut mirror_data)?;
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

    /// 固定次元の行構造体を使用した高速なバッチ挿入を提供します。
    pub async fn insert_fixed<const N: usize>(
        &self,
        items: Vec<PulseCellPack<N>>,
    ) -> Result<(), OrbyError> {
        let mut aof_data = Vec::with_capacity(items.len() * N * 16);
        let mut mirror_data = Vec::new();

        let (aof_sender, mirror_sender) = {
            let mut store = self.inner.write();
            let logic_mode = store.logic_mode;
            match logic_mode {
                LogicMode::Ring => {
                    ring::insert_fixed(&mut store, items, &mut aof_data, &mut mirror_data)?;
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

    /// 条件に一致するデータを一件ずつ取得するためのイテレータを生成します。
    pub fn query_iter<'a, F>(&'a self, filter: F) -> OrbyIterator<'a, F>
    where
        F: Fn(&[PulseCell]) -> bool,
    {
        let store = self.inner.read();
        let logic_mode = store.logic_mode;
        let head = store.head;
        let stride = store.stride;
        let cap = store.capacity;
        let len = store.len;

        let file = if store.buffer.is_empty() {
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
            head,
            stride,
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
            LogicMode::Ring => ring::query_raw(&store, filter, limit),
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
                LogicMode::Ring => {
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
                LogicMode::Ring => ring::update_by_id(
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

    /// ID が存在すれば更新、なければ新規挿入します。
    pub async fn upsert(&self, index: usize, id: u128, data: &[u128]) -> Result<(), OrbyError> {
        let mut aof_data = Vec::new();
        let mut mirror_data = Vec::new();

        let (aof_sender, mirror_sender) = {
            let mut store = self.inner.write();
            let logic_mode = store.logic_mode;
            match logic_mode {
                LogicMode::Ring => {
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
            LogicMode::Ring => ring::find_indices(&store, filter, limit),
        }
    }

    pub fn get_at(&self, logical_index: usize) -> Option<Arc<[u128]>> {
        let store = self.inner.read();
        match store.logic_mode {
            LogicMode::Ring => ring::get_at(&store, logical_index),
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
                LogicMode::Ring => ring::truncate(
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
