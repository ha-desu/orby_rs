use crate::error::OrbyError;
use crate::logic::OrbyRingBufferSilo;
use crate::row::PulseCellPack;
use crate::types::PulseCell;
use rayon::prelude::*;
use std::sync::Arc;

/// リングバッファ戦略に基づくバッチ挿入ロジック。
/// Parallel Arrays（次元ごとの独立した配列）の cursor 位置へ、データを垂直に焼き付けます。
pub fn insert_batch<T, I>(
    store: &mut OrbyRingBufferSilo,
    items: I,
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> Result<(), OrbyError>
where
    I: Iterator<Item = T>,
    T: AsRef<[u128]>,
{
    let dim = store.ring_buffer_lane_count;
    let cap = store.capacity;
    let has_aof = store.aof_sender.is_some();
    let has_mirror = store.mirror_sender.is_some();

    // メモリバッファが有効（非 StorageOnly）かチェック
    let has_mem = !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty();

    for item in items {
        let slice = item.as_ref();

        // 次元不一致チェック
        if slice.len() != dim {
            return Err(OrbyError::LaneCountMismatch {
                pool_name: store.name.clone(),
                expected: dim,
                found: slice.len(),
            });
        }

        // 1. AOF 用のログを生成
        if has_aof {
            aof_data.push(crate::logic::AOF_OP_INSERT);
            for &val in slice {
                aof_data.extend_from_slice(&val.to_le_bytes());
            }
        }

        // 2. メモリへの書き込み (Parallel Arrays 構造)
        if has_mem {
            let cursor = store.cursor;

            // 上書き判定：現在のカーソル位置が非ゼロなら、有効なレコードを周回して上書きしたとみなす
            let is_overwrite = store.lanes[0].buffer[cursor].as_u128() != 0;

            for (col, &val) in slice.iter().enumerate() {
                store.lanes[col].buffer[cursor] = PulseCell::new(val);
            }

            // 実データ件数（len）の更新
            if !is_overwrite && store.len < cap {
                store.len += 1;
            }
        } else {
            // ストレージのみモードの場合、簡易的な len カウント
            if store.len < cap {
                store.len += 1;
            }
        }

        // 3. ミラー同期（AoS形式への変換を伴う物理オフセット計算）
        if has_mirror {
            let offset_bytes = crate::types::HEADER_SIZE + (store.cursor as u64 * dim as u64 * 16);
            let mut row_bytes = Vec::with_capacity(dim * 16);
            for &val in slice {
                row_bytes.extend_from_slice(&val.to_le_bytes());
            }
            mirror_data.push((offset_bytes, row_bytes));
        }

        // カーソルを進める（リングバッファ）
        store.cursor = (store.cursor + 1) % cap;

        // ヘッダー情報の同期予約
        if has_mirror {
            let len_bytes = (store.len as u64).to_le_bytes().to_vec();
            let cursor_bytes = (store.cursor as u64).to_le_bytes().to_vec();
            mirror_data.push((24, len_bytes)); // header offset 24: len
            mirror_data.push((32, cursor_bytes)); // header offset 32: cursor
        }
    }
    Ok(())
}

/// 固定次元の `PulseCellPack` を使用した高速挿入ロジック。
/// `insert_batch` と同等のロジックをスタックアラインメント済みの構造体に対して実行します。
pub fn insert_fixed<const N: usize>(
    store: &mut OrbyRingBufferSilo,
    items: Vec<PulseCellPack<N>>,
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> Result<(), OrbyError> {
    let dim = store.ring_buffer_lane_count;
    if N != dim {
        return Err(OrbyError::LaneCountMismatch {
            pool_name: store.name.clone(),
            expected: dim,
            found: N,
        });
    }

    let cap = store.capacity;
    let has_aof = store.aof_sender.is_some();
    let has_mirror = store.mirror_sender.is_some();
    let has_mem = !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty();

    for item in items {
        if has_aof {
            aof_data.push(crate::logic::AOF_OP_INSERT);
            for &val in &item.values {
                aof_data.extend_from_slice(&val.as_u128().to_le_bytes());
            }
        }

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

        if has_mirror {
            let offset_bytes = crate::types::HEADER_SIZE + (store.cursor as u64 * dim as u64 * 16);
            let mut row_bytes = Vec::with_capacity(dim * 16);
            for &val in &item.values {
                row_bytes.extend_from_slice(&val.as_u128().to_le_bytes());
            }
            mirror_data.push((offset_bytes, row_bytes));
        }

        store.cursor = (store.cursor + 1) % cap;

        if has_mirror {
            let len_bytes = (store.len as u64).to_le_bytes().to_vec();
            let cursor_bytes = (store.cursor as u64).to_le_bytes().to_vec();
            mirror_data.push((24, len_bytes));
            mirror_data.push((32, cursor_bytes));
        }
    }
    Ok(())
}

/// 特定のレーン（次元）に対して、複数のパルスを一括で流し込みます。
/// 他のレーンとの整合性を取るため、同一インデックスの他レーンはゼロクリアされます。
pub fn insert_lane_batch(
    store: &mut OrbyRingBufferSilo,
    lane_idx: usize,
    values: &[u128],
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> Result<(), OrbyError> {
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

    // 1. AOF ログ記録
    if store.aof_sender.is_some() {
        aof_data.push(crate::logic::AOF_OP_LANE_BATCH);
        aof_data.extend_from_slice(&(lane_idx as u32).to_le_bytes());
        aof_data.extend_from_slice(&(count as u32).to_le_bytes());
        for &v in values {
            aof_data.extend_from_slice(&v.to_le_bytes());
        }
    }

    // 2. メモリ更新 (SoA 構造なので、特定レーンの slice に一括コピー可能)
    let has_mem = !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty();
    if has_mem {
        let start_cursor = store.cursor;

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

        // 他レーンのゼロクリア（垂直同期）
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

        // len の更新
        store.len = (store.len + count).min(cap);
    } else {
        store.len = (store.len + count).min(cap);
    }

    // ミラー同期
    if store.mirror_sender.is_some() {
        for i in 0..count {
            let physical_idx = (store.cursor + i) % cap;
            let offset = crate::types::HEADER_SIZE + (physical_idx as u64 * dim as u64 * 16);
            let mut row = vec![0u8; dim * 16];
            row[lane_idx * 16..(lane_idx + 1) * 16].copy_from_slice(&values[i].to_le_bytes());
            mirror_data.push((offset, row));
        }
        mirror_data.push((24, (store.len as u64).to_le_bytes().to_vec()));
        let final_cursor = (store.cursor + count) % cap;
        mirror_data.push((32, (final_cursor as u64).to_le_bytes().to_vec()));
    }

    // カーソル更新
    store.cursor = (store.cursor + count) % cap;

    Ok(())
}

/// プールの状態を完全にリセットし、指定されたデータで再初期化します。
pub fn truncate<T, I>(
    store: &mut OrbyRingBufferSilo,
    items: I,
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> Result<(), OrbyError>
where
    I: Iterator<Item = T>,
    T: AsRef<[u128]>,
{
    let dim = store.ring_buffer_lane_count;
    let cap = store.capacity;
    let has_aof = store.aof_sender.is_some();
    let has_mirror = store.mirror_sender.is_some();

    // 1. AOF への truncate 記録
    if has_aof {
        aof_data.push(crate::logic::AOF_OP_TRUNCATE);
    }

    // 2. メモリバッファをゼロクリア
    if !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty() {
        for lane in &mut store.lanes {
            lane.buffer.fill(PulseCell::new(0));
        }
    }
    store.len = 0;
    store.cursor = 0;

    // 3. 新しいデータの挿入
    let has_mem = !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty();

    for item in items.take(cap) {
        let slice = item.as_ref();
        if slice.len() != dim {
            return Err(OrbyError::LaneCountMismatch {
                pool_name: store.name.clone(),
                expected: dim,
                found: slice.len(),
            });
        }

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

    // 4. ミラー同期（全件書き換えに等しいためバルク送信）
    if has_mirror {
        if !store.lanes.is_empty() {
            let mut all_data = Vec::with_capacity(cap * dim * 16);
            for i in 0..cap {
                for col in 0..dim {
                    let val = store.lanes[col].buffer[i].as_u128();
                    all_data.extend_from_slice(&val.to_le_bytes());
                }
            }
            mirror_data.push((crate::types::HEADER_SIZE, all_data));
        }

        let len_bytes = (store.len as u64).to_le_bytes().to_vec();
        let cursor_bytes = (store.cursor as u64).to_le_bytes().to_vec();
        mirror_data.push((24, len_bytes));
        mirror_data.push((32, cursor_bytes));
    }

    Ok(())
}

/// 指定したインデックスのデータを削除します。
/// `compaction: true` の場合、後続のデータを前方へシフトし、常に隙間のない状態を維持します。
pub fn delete(
    store: &mut OrbyRingBufferSilo,
    index: usize,
    _aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> bool {
    if index >= store.capacity {
        return false;
    }
    let has_mem = !store.lanes.is_empty() && !store.lanes[0].buffer.is_empty();
    if !has_mem {
        // メモリバッファ未構築時は操作不能
        return false;
    }

    // 対象インデックスに有効なデータがあるか確認
    if store.lanes[0].buffer[index].as_u128() == 0 {
        return false;
    }

    // 1. インデックス位置のゼロクリア
    for lane in &mut store.lanes {
        lane.buffer[index] = PulseCell::new(0);
    }
    if store.len > 0 {
        store.len -= 1;
    }

    let has_mirror = store.mirror_sender.is_some();

    // 2. コンパクション（前方シフト）ロジック
    if store.compaction {
        let cap = store.capacity;

        // [index+1 .. cap] の範囲を [index .. cap-1] へ垂直コピー
        if index < cap - 1 {
            for lane in &mut store.lanes {
                lane.buffer.copy_within(index + 1..cap, index);
                // シフト後に残る末尾の重複データをゼロ埋め
                lane.buffer[cap - 1] = PulseCell::new(0);
            }
        }
        // コンパクション時は常に len の位置が書き込み位置（末尾）となる
        store.cursor = store.len;

        // 【注意】Compaction ON の時の AOF/Mirror 同期は複雑なため、
        // 物理レイヤー（Vault 等）への指示は Orby::delete メソッド側で補完されます。
    }

    // ミラーヘッダーの更新
    if has_mirror {
        let len_bytes = (store.len as u64).to_le_bytes().to_vec();
        let cursor_bytes = (store.cursor as u64).to_le_bytes().to_vec();
        mirror_data.push((24, len_bytes));
        mirror_data.push((32, cursor_bytes));
    }

    true
}

/// 指定した ID を持つ行を、メモリ上の物理位置を変えずに更新します。
pub fn update_by_id(
    store: &mut OrbyRingBufferSilo,
    index: usize,
    id: u128,
    new_data: &[u128],
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> bool {
    if id == 0 || new_data.len() != store.ring_buffer_lane_count {
        return false;
    }
    if index >= store.ring_buffer_lane_count {
        return false;
    }

    let mut found_any = false;
    let has_aof = store.aof_sender.is_some();
    let has_mirror = store.mirror_sender.is_some();

    // ID の所在を検索
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
        return false;
    }

    for physical_idx in targets {
        // AOF ログ記録
        if has_aof && !found_any {
            aof_data.push(crate::logic::AOF_OP_UPDATE);
            aof_data.extend_from_slice(&(index as u32).to_le_bytes());
            aof_data.extend_from_slice(&id.to_le_bytes());
            for &v in new_data {
                aof_data.extend_from_slice(&v.to_le_bytes());
            }
        }

        // 垂直書き込み（すべての次元の該当位置を一斉に書き換え）
        for (col, lane) in store.lanes.iter_mut().enumerate() {
            lane.buffer[physical_idx] = PulseCell::new(new_data[col]);
        }

        // ミラー同期
        if has_mirror {
            let offset_bytes = crate::types::HEADER_SIZE
                + (physical_idx as u64 * store.ring_buffer_lane_count as u64 * 16);
            let mut row_bytes = Vec::with_capacity(new_data.len() * 16);
            for &v in new_data {
                row_bytes.extend_from_slice(&v.to_le_bytes());
            }
            mirror_data.push((offset_bytes, row_bytes));
        }

        found_any = true;
    }
    found_any
}

/// 指定した ID があれば更新、なければ挿入します。
pub fn upsert(
    store: &mut OrbyRingBufferSilo,
    index: usize,
    id: u128,
    data: &[u128],
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> Result<(), OrbyError> {
    if !update_by_id(store, index, id, data, aof_data, mirror_data) {
        insert_batch(store, std::iter::once(data), aof_data, mirror_data)?;
    }
    Ok(())
}

/// 特定のカラム値に一致するレコードを検索し、その場でゼロ埋め（削除）します。
pub fn purge_by_id(
    store: &mut OrbyRingBufferSilo,
    index: usize,
    id: u128,
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) {
    if id == 0 || index >= store.ring_buffer_lane_count {
        return;
    }
    if store.aof_sender.is_some() {
        aof_data.push(crate::logic::AOF_OP_PURGE);
        aof_data.extend_from_slice(&(index as u32).to_le_bytes());
        aof_data.extend_from_slice(&id.to_le_bytes());
    }

    let has_mirror = store.mirror_sender.is_some();
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

    for physical_idx in targets {
        // レーンを垂直にゼロ埋め（この段階では詰め処理は行わない）
        for lane in &mut store.lanes {
            lane.buffer[physical_idx] = PulseCell::new(0);
        }

        if store.len > 0 {
            store.len -= 1;
        }

        if has_mirror {
            let offset_bytes = crate::types::HEADER_SIZE
                + (physical_idx as u64 * store.ring_buffer_lane_count as u64 * 16);
            let zeros = vec![0u8; store.ring_buffer_lane_count * 16];
            mirror_data.push((offset_bytes, zeros));

            // ヘッダーの len 同期
            let len_bytes = (store.len as u64).to_le_bytes().to_vec();
            mirror_data.push((24, len_bytes));
        }
    }
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
