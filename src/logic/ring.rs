use crate::error::OrbyError;
use crate::logic::OrbyStore;
use crate::row::OrbyRow;
use crate::types::OrbitField;
use crate::utils::cache::calculate_min_len;
use rayon::prelude::*;
use std::sync::Arc;

/// リングバッファ戦略に基づく挿入ロジック。
pub fn insert_batch<T, I>(
    store: &mut OrbyStore,
    items: I,
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> Result<(), OrbyError>
where
    I: Iterator<Item = T>,
    T: AsRef<[u128]>,
{
    let dim = store.dimension;
    let padded_dim = store.padded_dimension;
    let cap = store.capacity;
    let has_aof = store.aof_sender.is_some();
    let has_mirror = store.mirror_sender.is_some();

    for item in items {
        let slice = item.as_ref();

        if slice.len() != dim {
            return Err(OrbyError::DimensionMismatch {
                pool_name: store.name.clone(),
                expected: dim,
                found: slice.len(),
            });
        }

        if has_aof {
            aof_data.push(crate::logic::AOF_OP_INSERT);
            for &val in slice {
                aof_data.extend_from_slice(&val.to_le_bytes());
            }
        }

        let start = store.head * padded_dim;
        if !store.buffer.is_empty() {
            for (i, &val) in slice.iter().enumerate() {
                store.buffer[start + i] = OrbitField::new(val);
            }
        }

        if has_mirror {
            let offset_bytes = crate::types::HEADER_SIZE + (start * 16) as u64;
            let mut row_bytes = Vec::with_capacity(dim * 16);
            for &val in slice {
                row_bytes.extend_from_slice(&val.to_le_bytes());
            }
            mirror_data.push((offset_bytes, row_bytes));
        }

        store.head = (store.head + 1) % cap;
        if store.len < cap {
            store.len += 1;
        }

        if has_mirror {
            // Sync Metadata (Head: 48, Len: 40)
            let len_bytes = (store.len as u64).to_le_bytes().to_vec();
            let head_bytes = (store.head as u64).to_le_bytes().to_vec();
            mirror_data.push((40, len_bytes));
            mirror_data.push((48, head_bytes));
        }
    }
    Ok(())
}

pub fn insert_fixed<const N: usize>(
    store: &mut OrbyStore,
    items: Vec<OrbyRow<N>>,
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> Result<(), OrbyError> {
    let dim = store.dimension;
    let padded_dim = store.padded_dimension;
    let cap = store.capacity;
    let has_aof = store.aof_sender.is_some();
    let has_mirror = store.mirror_sender.is_some();

    if N != dim {
        return Err(OrbyError::DimensionMismatch {
            pool_name: store.name.clone(),
            expected: dim,
            found: N,
        });
    }

    for item in items {
        if has_aof {
            aof_data.push(crate::logic::AOF_OP_INSERT);
            for &val in &item.values {
                aof_data.extend_from_slice(&val.as_u128().to_le_bytes());
            }
        }
        let start = store.head * padded_dim;
        if !store.buffer.is_empty() {
            for (i, &val) in item.values.iter().enumerate() {
                store.buffer[start + i] = val;
            }
        }

        if has_mirror {
            let offset_bytes = crate::types::HEADER_SIZE + (start * 16) as u64;
            let mut row_bytes = Vec::with_capacity(dim * 16);
            for &val in &item.values {
                row_bytes.extend_from_slice(&val.as_u128().to_le_bytes());
            }
            mirror_data.push((offset_bytes, row_bytes));
        }

        store.head = (store.head + 1) % cap;
        if store.len < cap {
            store.len += 1;
        }

        if has_mirror {
            let len_bytes = (store.len as u64).to_le_bytes().to_vec();
            let head_bytes = (store.head as u64).to_le_bytes().to_vec();
            mirror_data.push((40, len_bytes));
            mirror_data.push((48, head_bytes));
        }
    }
    Ok(())
}

/// プールの状態をリセットし、新しいデータで満たします。
pub fn truncate<T, I>(
    store: &mut OrbyStore,
    items: I,
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> Result<(), OrbyError>
where
    I: Iterator<Item = T>,
    T: AsRef<[u128]>,
{
    let dim = store.dimension;
    let padded_dim = store.padded_dimension;
    let cap = store.capacity;
    let has_aof = store.aof_sender.is_some();
    let has_mirror = store.mirror_sender.is_some();

    // 1. AOF
    if has_aof {
        aof_data.push(crate::logic::AOF_OP_TRUNCATE);
    }

    // 2. Memory Reset
    if !store.buffer.is_empty() {
        store.buffer.fill(OrbitField::new(0));
    }
    store.len = 0;
    store.head = 0;

    // 3. Insert new data
    for item in items.take(cap) {
        let slice = item.as_ref();
        if slice.len() != dim {
            return Err(OrbyError::DimensionMismatch {
                pool_name: store.name.clone(),
                expected: dim,
                found: slice.len(),
            });
        }

        let start = store.head * padded_dim;
        if !store.buffer.is_empty() {
            for (i, &val) in slice.iter().enumerate() {
                store.buffer[start + i] = OrbitField::new(val);
            }
        }

        store.head = (store.head + 1) % cap;
        if store.len < cap {
            store.len += 1;
        }
    }

    if has_mirror {
        if !store.buffer.is_empty() {
            let mut all_data = Vec::with_capacity(cap * padded_dim * 16);
            for field in &store.buffer {
                all_data.extend_from_slice(&field.as_u128().to_le_bytes());
            }
            mirror_data.push((crate::types::HEADER_SIZE, all_data));
        }

        // Sync Metadata
        let len_bytes = (store.len as u64).to_le_bytes().to_vec();
        let head_bytes = (store.head as u64).to_le_bytes().to_vec();
        mirror_data.push((40, len_bytes));
        mirror_data.push((48, head_bytes));
    }

    Ok(())
}

/// 指定した ID を持つ行を、メモリ位置を変えずにその場で更新します。
/// 見つかった場合は true、見つからなかった場合は false を返します。
pub fn update_by_id(
    store: &mut OrbyStore,
    index: usize,
    id: u128,
    new_data: &[u128],
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) -> bool {
    if id == 0 || new_data.len() != store.dimension {
        return false;
    }

    let mut found_any = false;
    let padded_dim = store.padded_dimension;
    let has_aof = store.aof_sender.is_some();
    let has_mirror = store.mirror_sender.is_some();

    for i in 0..store.len {
        let start = i * padded_dim;
        if let Some(val) = store.buffer.get(start + index) {
            if val.as_u128() == id {
                if has_aof && !found_any {
                    aof_data.push(crate::logic::AOF_OP_UPDATE);
                    aof_data.extend_from_slice(&(index as u32).to_le_bytes());
                    aof_data.extend_from_slice(&id.to_le_bytes());
                    for &v in new_data {
                        aof_data.extend_from_slice(&v.to_le_bytes());
                    }
                }

                for (offset, &v) in new_data.iter().enumerate() {
                    store.buffer[start + offset] = OrbitField::new(v);
                }

                if has_mirror {
                    // Update entire row
                    let offset_bytes = crate::types::HEADER_SIZE + (start * 16) as u64;
                    let mut row_bytes = Vec::with_capacity(new_data.len() * 16);
                    for &v in new_data {
                        row_bytes.extend_from_slice(&v.to_le_bytes());
                    }
                    mirror_data.push((offset_bytes, row_bytes));
                }

                found_any = true;
            }
        }
    }
    found_any
}

/// Upsert ロジック: 既存 ID があれば Update、なければ Insert (最新位置) を行います。
pub fn upsert(
    store: &mut OrbyStore,
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

/// 指定した ID を持つ行をゼロ埋めします。
pub fn purge_by_id(
    store: &mut OrbyStore,
    index: usize,
    id: u128,
    aof_data: &mut Vec<u8>,
    mirror_data: &mut Vec<(u64, Vec<u8>)>,
) {
    if id == 0 {
        return;
    }
    if store.aof_sender.is_some() {
        aof_data.push(crate::logic::AOF_OP_PURGE);
        aof_data.extend_from_slice(&(index as u32).to_le_bytes());
        aof_data.extend_from_slice(&id.to_le_bytes());
    }

    let padded_dim = store.padded_dimension;
    let len = store.len;
    let has_mirror = store.mirror_sender.is_some();

    for i in 0..len {
        let start = i * padded_dim;
        if let Some(val) = store.buffer.get(start + index) {
            if val.as_u128() == id {
                store.buffer[start..start + padded_dim].fill(OrbitField::new(0));

                if has_mirror {
                    // Write zeros to file
                    let offset_bytes = crate::types::HEADER_SIZE + (start * 16) as u64;
                    let zeros = vec![0u8; padded_dim * 16];
                    mirror_data.push((offset_bytes, zeros));
                }
            }
        }
    }
}

/// 最新から数えて n 番目のレコードを 1 件取得します。
pub fn get_at(store: &OrbyStore, logical_index: usize) -> Option<Arc<[u128]>> {
    if logical_index >= store.len {
        return None;
    }
    let head = store.head;
    let cap = store.capacity;
    let physical_idx = if head > logical_index {
        head - 1 - logical_index
    } else {
        cap + head - 1 - logical_index
    };

    if store.buffer.is_empty() {
        // StorageOnly mode: Read from file
        if let Some(path) = &store.mirror_path {
            use std::io::{Read, Seek};
            let mut f = std::fs::File::open(path).ok()?;
            let offset =
                crate::types::HEADER_SIZE + (physical_idx * store.padded_dimension * 16) as u64;
            if f.seek(std::io::SeekFrom::Start(offset)).is_err() {
                return None;
            }

            let mut buf = vec![0u8; store.dimension * 16];
            // We only read store.dimension items, but file has padded_dimension.
            // They are contiguous at start of row.
            if f.read_exact(&mut buf).is_err() {
                return None;
            }

            let mut data = Vec::with_capacity(store.dimension);
            for chunk in buf.chunks_exact(16) {
                // Try into array
                if let Ok(bytes) = chunk.try_into() {
                    data.push(u128::from_le_bytes(bytes));
                }
            }
            return Some(Arc::from(data));
        }
        return None;
    }

    let start = physical_idx * store.padded_dimension;
    let row_data = &store.buffer[start..start + store.dimension];

    Some(Arc::from(
        row_data.iter().map(|v| v.as_u128()).collect::<Vec<_>>(),
    ))
}

pub fn query_raw<F>(store: &OrbyStore, filter: F, limit: usize) -> Vec<Arc<[u128]>>
where
    F: Fn(&[OrbitField]) -> bool + Sync + Send,
{
    let dim = store.dimension;
    let padded_dim = store.padded_dimension;
    let len = store.len;
    let head = store.head;
    let cap = store.capacity;

    if store.buffer.is_empty() {
        if let Some(path) = &store.mirror_path {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = match std::fs::File::open(path) {
                Ok(f) => f,
                Err(_) => return Vec::new(),
            };
            let mut results = Vec::new();

            // Ring logic order: head-1..0 then cap-1..head
            let mut order = Vec::new();
            for i in (0..head).rev() {
                order.push(i);
            }
            if len == cap {
                for i in (head..cap).rev() {
                    order.push(i);
                }
            }

            for i in order {
                let offset = crate::types::HEADER_SIZE + (i * padded_dim * 16) as u64;
                if f.seek(SeekFrom::Start(offset)).is_err() {
                    break;
                }
                let mut buf = vec![0u8; dim * 16];
                if f.read_exact(&mut buf).is_err() {
                    break;
                }
                let orbit_fields: Vec<OrbitField> = buf
                    .chunks_exact(16)
                    .map(|c| OrbitField::new(u128::from_le_bytes(c.try_into().unwrap())))
                    .collect();

                if filter(&orbit_fields) {
                    results.push(Arc::from(
                        orbit_fields
                            .into_iter()
                            .map(|f| f.as_u128())
                            .collect::<Vec<_>>(),
                    ));
                    if results.len() >= limit {
                        break;
                    }
                }
            }
            return results;
        }
        return Vec::new();
    }

    let min_len = calculate_min_len(padded_dim);

    let (front, back) = store.buffer[..len * padded_dim].split_at(head * padded_dim);
    let mut results: Vec<Arc<[u128]>> = front
        .par_chunks_exact(padded_dim)
        .rev()
        .chain(back.par_chunks_exact(padded_dim).rev())
        .with_min_len(min_len)
        .filter_map(|row| {
            let row_data = &row[..dim];
            if row_data.iter().all(|&v| v.as_u128() == 0) {
                return None;
            }

            if filter(row_data) {
                Some(Arc::from(
                    row_data
                        .iter()
                        .map(|b| b.as_u128())
                        .collect::<Vec<_>>()
                        .into_boxed_slice(),
                ))
            } else {
                None
            }
        })
        .collect();
    results.truncate(limit);
    results
}

pub fn find_indices<F>(store: &OrbyStore, filter: F, limit: usize) -> Vec<usize>
where
    F: Fn(&[OrbitField]) -> bool + Sync + Send,
{
    let dim = store.dimension;
    let padded_dim = store.padded_dimension;
    let len = store.len;
    let head = store.head;
    let cap = store.capacity;

    if store.buffer.is_empty() {
        if let Some(path) = &store.mirror_path {
            use std::io::{Read, Seek, SeekFrom};
            let mut f = match std::fs::File::open(path) {
                Ok(f) => f,
                Err(_) => return Vec::new(),
            };
            let mut indices = Vec::new();
            // Use same logic as query_raw results
            let mut order = Vec::new();
            for i in (0..head).rev() {
                order.push(i);
            }
            if len == cap {
                for i in (head..cap).rev() {
                    order.push(i);
                }
            }

            for (logical_idx, physical_idx) in order.into_iter().enumerate() {
                let offset = crate::types::HEADER_SIZE + (physical_idx * padded_dim * 16) as u64;
                if f.seek(SeekFrom::Start(offset)).is_err() {
                    break;
                }
                let mut buf = vec![0u8; dim * 16];
                if f.read_exact(&mut buf).is_err() {
                    break;
                }
                let orbit_fields: Vec<OrbitField> = buf
                    .chunks_exact(16)
                    .map(|c| OrbitField::new(u128::from_le_bytes(c.try_into().unwrap())))
                    .collect();

                if filter(&orbit_fields) {
                    indices.push(logical_idx);
                    if indices.len() >= limit {
                        break;
                    }
                }
            }
            return indices;
        }
        return Vec::new();
    }

    let min_len = calculate_min_len(padded_dim);

    // 有効な物理領域全体を並列スキャン
    // 注意: リングバッファでは物理順序 != 論理順序 なので、
    // ここで得られる結果は「論理順序（最新順）」にはなりません。
    // 後でソートする必要があります。

    let mut indices: Vec<usize> = store.buffer[..len * padded_dim]
        .par_chunks_exact(padded_dim)
        .enumerate() // 物理インデックス
        .with_min_len(min_len)
        .filter_map(|(p, row)| {
            let row_data = &row[..dim];
            if row_data.iter().all(|&v| v.as_u128() == 0) {
                return None;
            }

            if filter(row_data) {
                // 物理 -> 論理インデックス変換
                let logical_idx = if p < head {
                    head - 1 - p
                } else {
                    cap + head - 1 - p
                };
                Some(logical_idx)
            } else {
                None
            }
        })
        .collect();

    // 論理インデックス(最新順)でソート
    indices.par_sort_unstable();
    indices.truncate(limit);
    indices
}
