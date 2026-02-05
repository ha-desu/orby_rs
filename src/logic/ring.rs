use crate::error::OrbyError;
use crate::logic::{OrbyRingBufferSilo, PersistenceChanges, RingOperation};
use crate::row::PulseCellPack;
use crate::types::PulseCell;
use rayon::prelude::*;
use std::sync::Arc;

/// リングバッファ戦略に基づくバッチ挿入ロジック。
/// Parallel Arrays（次元ごとの独立した配列）の cursor 位置へ、データを垂直に焼き付けます。
pub fn insert_batch<T, I>(
    store: &mut OrbyRingBufferSilo,
    items: I,
) -> Result<PersistenceChanges, OrbyError>
where
    I: Iterator<Item = T>,
    T: AsRef<[u128]>,
{
    let mut changes = PersistenceChanges::new();
    let dim = store.ring_buffer_lane_count;
    let cap = store.capacity;

    // Convert items once to use in both memory update and event
    let raw_rows: Vec<Vec<u128>> = items.map(|item| item.as_ref().to_vec()).collect();
    if raw_rows.is_empty() {
        return Ok(changes);
    }

    // 1. メモリバッファが有効（非 StorageOnly）かチェック
    let has_mem = !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty();
    let start_cursor = store.cursor;

    for row in &raw_rows {
        // 次元不一致チェック
        if row.len() != dim {
            return Err(OrbyError::LaneCountMismatch {
                pool_name: store.name.clone(),
                expected: dim,
                found: row.len(),
            });
        }

        // 2. メモリへの書き込み (Parallel Arrays 構造)
        if has_mem {
            let cursor = store.cursor;
            let is_overwrite = store.lanes[0].buffer[cursor].as_u128() != 0;
            for (col, &val) in row.iter().enumerate() {
                store.lanes[col].buffer[cursor] = PulseCell::new(val);
            }
            if !is_overwrite && store.len < cap {
                store.len += 1;
            }
        } else if store.len < cap {
            store.len += 1;
        }

        // カーソルを進める（リングバッファ）
        store.cursor = (store.cursor + 1) % cap;
    }

    // イベントの記録
    changes.push(RingOperation::Insert {
        cursor: start_cursor,
        row_count: raw_rows.len(),
        data: raw_rows,
    });
    changes.push(RingOperation::HeaderUpdate {
        len: store.len,
        cursor: store.cursor,
    });

    Ok(changes)
}

/// 固定次元の `PulseCellPack` を使用した高速挿入ロジック。
/// `insert_batch` と同等のロジックをスタックアラインメント済みの構造体に対して実行します。
pub fn insert_fixed<const N: usize>(
    store: &mut OrbyRingBufferSilo,
    items: Vec<PulseCellPack<N>>,
) -> Result<PersistenceChanges, OrbyError> {
    let mut changes = PersistenceChanges::new();
    let dim = store.ring_buffer_lane_count;
    if N != dim {
        return Err(OrbyError::LaneCountMismatch {
            pool_name: store.name.clone(),
            expected: dim,
            found: N,
        });
    }

    let cap = store.capacity;
    let has_mem = !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty();
    let start_cursor = store.cursor;
    let row_count = items.len();

    let mut raw_data = Vec::with_capacity(row_count);

    for item in items {
        let row: Vec<u128> = item.values.iter().map(|v| v.as_u128()).collect();
        raw_data.push(row.clone());

        if has_mem {
            let cursor = store.cursor;
            let is_overwrite = store.lanes[0].buffer[cursor].as_u128() != 0;

            for (col, &val) in item.values.iter().enumerate() {
                store.lanes[col].buffer[cursor] = val;
            }

            if !is_overwrite && store.len < cap {
                store.len += 1;
            }
        } else if store.len < cap {
            store.len += 1;
        }

        store.cursor = (store.cursor + 1) % cap;
    }

    changes.push(RingOperation::Insert {
        cursor: start_cursor,
        row_count,
        data: raw_data,
    });
    changes.push(RingOperation::HeaderUpdate {
        len: store.len,
        cursor: store.cursor,
    });

    Ok(changes)
}

/// 特定のレーン（次元）に対して、複数のパルスを一括で流し込みます。
/// 他のレーンとの整合性を取るため、同一インデックスの他レーンはゼロクリアされます。
pub fn insert_lane_batch(
    store: &mut OrbyRingBufferSilo,
    lane_idx: usize,
    values: &[u128],
) -> Result<PersistenceChanges, OrbyError> {
    let mut changes = PersistenceChanges::new();
    let dim = store.ring_buffer_lane_count;
    let cap = store.capacity;
    let count = values.len();

    if lane_idx >= dim {
        return Err(OrbyError::LaneCountMismatch {
            pool_name: store.name.clone(),
            expected: dim,
            found: lane_idx + 1,
        });
    }

    if count > cap {
        return Err(OrbyError::StorageFull {
            pool_name: store.name.clone(),
            capacity: cap,
        });
    }

    let start_cursor = store.cursor;

    // 1. メモリ更新
    let has_mem = !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty();
    if has_mem {
        // ターゲットレーンの更新（ラップアラウンド考慮）
        {
            let buffer = &mut store.lanes[lane_idx].buffer;
            if start_cursor + count <= cap {
                for i in 0..count {
                    buffer[start_cursor + i] = PulseCell::new(values[i]);
                }
            } else {
                let len_to_end = cap - start_cursor;
                for i in 0..len_to_end {
                    buffer[start_cursor + i] = PulseCell::new(values[i]);
                }
                for i in 0..(count - len_to_end) {
                    buffer[i] = PulseCell::new(values[len_to_end + i]);
                }
            }
        }

        // 他レーンのゼロクリア
        for col in 0..dim {
            if col == lane_idx {
                continue;
            }
            let buffer = &mut store.lanes[col].buffer;
            if start_cursor + count <= cap {
                for i in 0..count {
                    buffer[start_cursor + i] = PulseCell::new(0);
                }
            } else {
                let len_to_end = cap - start_cursor;
                for i in 0..len_to_end {
                    buffer[start_cursor + i] = PulseCell::new(0);
                }
                for i in 0..(count - len_to_end) {
                    buffer[i] = PulseCell::new(0);
                }
            }
        }
    }

    store.len = (store.len + count).min(cap);
    store.cursor = (store.cursor + count) % cap;

    // 2. イベント記録
    changes.push(RingOperation::LaneBatch {
        lane_idx,
        start_cursor,
        values: values.to_vec(),
    });
    changes.push(RingOperation::HeaderUpdate {
        len: store.len,
        cursor: store.cursor,
    });

    Ok(changes)
}

/// プールの状態を完全にリセットし、指定されたデータで再初期化します。
pub fn truncate<T, I>(
    store: &mut OrbyRingBufferSilo,
    items: I,
) -> Result<PersistenceChanges, OrbyError>
where
    I: Iterator<Item = T>,
    T: AsRef<[u128]>,
{
    let mut changes = PersistenceChanges::new();
    let dim = store.ring_buffer_lane_count;
    let cap = store.capacity;

    // 1. メモリバッファをゼロクリア
    if !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty() {
        for lane in &mut store.lanes {
            lane.buffer.fill(PulseCell::new(0));
        }
    }
    store.len = 0;
    store.cursor = 0;

    // 2. 新しいデータの挿入
    let has_mem = !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty();
    let mut new_rows = Vec::new();

    for item in items.take(cap) {
        let slice = item.as_ref();
        if slice.len() != dim {
            return Err(OrbyError::LaneCountMismatch {
                pool_name: store.name.clone(),
                expected: dim,
                found: slice.len(),
            });
        }
        new_rows.push(slice.to_vec());

        if has_mem {
            let cursor = store.cursor;
            for (col, &val) in slice.iter().enumerate() {
                store.lanes[col].buffer[cursor] = PulseCell::new(val);
            }
            store.len += 1;
        } else {
            store.len += 1;
        }

        store.cursor = (store.cursor + 1) % cap;
    }

    // 3. イベント記録
    changes.push(RingOperation::Truncate { new_rows });
    changes.push(RingOperation::HeaderUpdate {
        len: store.len,
        cursor: store.cursor,
    });

    Ok(changes)
}

/// 指定したインデックスのデータを削除します。
/// `compaction: true` の場合、後続のデータを前方へシフトし、常に隙間のない状態を維持します。
pub fn delete(store: &mut OrbyRingBufferSilo, index: usize) -> (bool, PersistenceChanges) {
    let mut changes = PersistenceChanges::new();
    if index >= store.capacity {
        return (false, changes);
    }
    let has_mem = !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty();
    if !has_mem {
        return (false, changes);
    }

    if store.lanes[0].buffer[index].as_u128() == 0 {
        return (false, changes);
    }

    // 1. インデックス位置のゼロクリア
    for lane in &mut store.lanes {
        lane.buffer[index] = PulseCell::new(0);
    }
    if store.len > 0 {
        store.len -= 1;
    }

    changes.push(RingOperation::Delete {
        physical_index: index,
    });

    // 2. コンパクション（前方シフト）
    if store.compaction {
        let cap = store.capacity;
        if index < cap - 1 {
            for lane in &mut store.lanes {
                lane.buffer.copy_within(index + 1..cap, index);
                lane.buffer[cap - 1] = PulseCell::new(0);
            }
        }
        store.cursor = store.len;
    }

    changes.push(RingOperation::HeaderUpdate {
        len: store.len,
        cursor: store.cursor,
    });

    (true, changes)
}

/// 指定した ID を持つ行を、メモリ上の物理位置を変えずに更新します。
pub fn update_by_id(
    store: &mut OrbyRingBufferSilo,
    index: usize,
    id: u128,
    new_data: &[u128],
) -> (bool, PersistenceChanges) {
    let mut changes = PersistenceChanges::new();
    if id == 0 || new_data.len() != store.ring_buffer_lane_count {
        return (false, changes);
    }
    if index >= store.ring_buffer_lane_count {
        return (false, changes);
    }

    let mut found_any = false;
    let mut targets = Vec::new();
    {
        let search_lane = &store.lanes[index];
        for physical_idx in 0..store.capacity {
            if search_lane.buffer[physical_idx].as_u128() == id {
                targets.push(physical_idx);
            }
        }
    }

    if targets.is_empty() {
        return (false, changes);
    }

    for physical_idx in targets {
        // 垂直書き込み
        for (col, lane) in store.lanes.iter_mut().enumerate() {
            lane.buffer[physical_idx] = PulseCell::new(new_data[col]);
        }

        changes.push(RingOperation::Update {
            physical_index: physical_idx,
            id,
            new_data: new_data.to_vec(),
            logical_column: index,
        });

        found_any = true;
    }
    (found_any, changes)
}

/// 指定した ID があれば更新、なければ挿入します。
pub fn upsert(
    store: &mut OrbyRingBufferSilo,
    index: usize,
    id: u128,
    data: &[u128],
) -> Result<PersistenceChanges, OrbyError> {
    let (found, mut changes) = update_by_id(store, index, id, data);
    if !found {
        changes = insert_batch(store, std::iter::once(data))?;
    }
    Ok(changes)
}

/// 特定のカラム値に一致するレコードを検索し、その場でゼロ埋め（削除）します。
pub fn purge_by_id(store: &mut OrbyRingBufferSilo, index: usize, id: u128) -> PersistenceChanges {
    let mut changes = PersistenceChanges::new();
    if id == 0 || index >= store.ring_buffer_lane_count {
        return changes;
    }

    let cap = store.capacity;
    let mut targets = Vec::new();
    {
        let search_lane = &store.lanes[index];
        for physical_idx in 0..cap {
            if search_lane.buffer[physical_idx].as_u128() == id {
                targets.push(physical_idx);
            }
        }
    }

    if targets.is_empty() {
        return changes;
    }

    for &physical_idx in &targets {
        for lane in &mut store.lanes {
            lane.buffer[physical_idx] = PulseCell::new(0);
        }

        if store.len > 0 {
            store.len -= 1;
        }
    }

    changes.push(RingOperation::Purge {
        physical_indices: targets,
        id,
        logical_column: index,
    });
    changes.push(RingOperation::HeaderUpdate {
        len: store.len,
        cursor: store.cursor,
    });

    changes
}

/// 実際にメモリ配列を走査し、非ゼロのパルス数をカウントします。
/// リングバッファの性質上、有効データは 0 から cursor/capacity の範囲に存在します。
pub fn count_active(store: &OrbyRingBufferSilo) -> usize {
    if store.lanes.is_empty() || store.lanes[0].buffer.is_empty() {
        return store.len;
    }

    // カーソルが周回している場合は全域、そうでない場合はカーソル位置までを走査範囲とする
    let limit = if store.cursor == 0 && store.len > 0 {
        store.capacity
    } else {
        store.cursor
    };

    let mut count = 0;
    // 第1レーン（プライマリ次元）を指標としてアクティブセルを走査
    let lane0 = &store.lanes[0].buffer;
    for i in 0..limit {
        if lane0[i].as_u128() != 0 {
            count += 1;
        }
    }
    count
}

/// 論理インデックス（最新順）から物理オフセットを計算し、生データを取得します。
pub fn get_at(store: &OrbyRingBufferSilo, logical_index: usize) -> Option<Arc<[u128]>> {
    if logical_index >= store.len {
        return None;
    }
    let cursor = store.cursor;
    let cap = store.capacity;

    // カーソル位置を最新パルスとみなし、逆算して物理インデックスを特定
    let physical_idx = if cursor > logical_index {
        cursor - 1 - logical_index
    } else {
        cap + cursor - 1 - logical_index
    };

    // メモリバッファが空の場合はミラーファイルから直接読み出し
    if store.lanes.is_empty() || store.lanes[0].buffer.is_empty() {
        if let Some(path) = &store.mirror_path {
            use std::io::{Read, Seek};
            let mut f = std::fs::File::open(path).ok()?;
            let offset = crate::types::HEADER_SIZE
                + (physical_idx * store.ring_buffer_lane_count * 16) as u64;
            if f.seek(std::io::SeekFrom::Start(offset)).is_err() {
                return None;
            }
            let mut buf = vec![0u8; store.ring_buffer_lane_count * 16];
            if f.read_exact(&mut buf).is_err() {
                return None;
            }
            let mut data = Vec::with_capacity(store.ring_buffer_lane_count);
            for chunk in buf.chunks_exact(16) {
                if let Ok(bytes) = chunk.try_into() {
                    data.push(u128::from_le_bytes(bytes));
                }
            }
            return Some(Arc::from(data));
        }
        return None;
    }

    // SoA 構造から一括取得して Arc 配列にパッケージ化
    let mut row_data = Vec::with_capacity(store.ring_buffer_lane_count);
    for col in 0..store.ring_buffer_lane_count {
        row_data.push(store.lanes[col].buffer[physical_idx].as_u128());
    }

    Some(Arc::from(row_data))
}

/// Rayon を使用した並列 SIMD 風スキャンを実行します。
/// 最新のものから順にフィルタリングを適用し、結果をパルス形式で返します。
pub fn query_raw<F>(store: &OrbyRingBufferSilo, filter: F, limit: usize) -> Vec<Arc<[u128]>>
where
    F: Fn(&[PulseCell]) -> bool + Sync + Send,
{
    let dim = store.ring_buffer_lane_count;
    let len = store.len;
    let cursor = store.cursor;
    let cap = store.capacity;

    if store.lanes.is_empty() || store.lanes[0].buffer.is_empty() {
        return Vec::new(); // ストレージ直接クエリはイテレータ側で処理
    }

    // キャッシュサイズ等に基づく最適な並列単位
    let min_len = 1024;

    // スキャン順序を決定（最新から順に取得するため、インデックス計算の順序を整理）
    let mut order = Vec::with_capacity(len);
    for i in (0..cursor).rev() {
        order.push(i);
    }
    if len == cap {
        for i in (cursor..cap).rev() {
            order.push(i);
        }
    }

    let matches: Vec<usize> = order
        .into_par_iter()
        .with_min_len(min_len)
        .filter(|&i| {
            let mut row_cells = Vec::with_capacity(dim);
            for col in 0..dim {
                row_cells.push(store.lanes[col].buffer[i]);
            }
            // 墓標（全次元ゼロ）はスキップ
            if row_cells.iter().all(|&v| v.as_u128() == 0) {
                return false;
            }
            filter(&row_cells)
        })
        .collect();

    let mut results = Vec::with_capacity(limit);
    for &i in matches.iter().take(limit) {
        let mut row_vals = Vec::with_capacity(dim);
        for col in 0..dim {
            row_vals.push(store.lanes[col].buffer[i].as_u128());
        }
        results.push(Arc::from(row_vals));
    }
    results
}

/// 条件に合致するレコードの論理インデックスリストを取得します。
pub fn find_indices<F>(store: &OrbyRingBufferSilo, filter: F, limit: usize) -> Vec<usize>
where
    F: Fn(&[PulseCell]) -> bool + Sync + Send,
{
    let dim = store.ring_buffer_lane_count;
    let len = store.len;
    let cursor = store.cursor;
    let cap = store.capacity;

    if store.lanes.is_empty() || store.lanes[0].buffer.is_empty() {
        return Vec::new();
    }

    let min_len = 1024;
    let mut order = Vec::with_capacity(len);
    for i in (0..cursor).rev() {
        order.push(i);
    }
    if len == cap {
        for i in (cursor..cap).rev() {
            order.push(i);
        }
    }

    let mut indices: Vec<usize> = order
        .into_par_iter()
        .enumerate()
        .with_min_len(min_len)
        .filter_map(|(logical_idx, physical_idx)| {
            let mut row_cells = Vec::with_capacity(dim);
            for col in 0..dim {
                row_cells.push(store.lanes[col].buffer[physical_idx]);
            }

            if row_cells.iter().all(|&v| v.as_u128() == 0) {
                return None;
            }

            if filter(&row_cells) {
                Some(logical_idx)
            } else {
                None
            }
        })
        .collect();

    // 安定ソートを行い、上位 limit 件を抽出
    indices.par_sort_unstable();
    indices.truncate(limit);
    indices
}
