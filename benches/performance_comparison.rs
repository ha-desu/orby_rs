use criterion::{black_box, criterion_group, criterion_main, Criterion};
use orby::{LogicMode, Orby, SaveMode};
use std::collections::HashSet;
use std::fs;
use std::path::PathBuf;
use tokio::runtime::Runtime;

/// 実行終了時（パニック含む）に一時ファイルを削除するためのガード
struct CleanupGuard;

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        let files = [
            "db_data/bench_sync_ring.orby",
            "db_data/bench_direct_ring.orby",
            "db_data/bench_sync_fixed.orby",
            "db_data/bench_direct_fixed.orby",
            "db_data/bench_sync_ring.aof",
            "db_data/bench_direct_ring.aof",
            "db_data/bench_sync_fixed.aof",
            "db_data/bench_direct_fixed.aof",
        ];
        for f in files {
            let _ = fs::remove_file(f);
        }
    }
}

// Generate dummy data
fn generate_dummy_data(
    user_count: usize,
    posts_per_user: usize,
    dimension: usize,
) -> (Vec<u128>, Vec<Vec<u128>>) {
    let mut users = Vec::with_capacity(user_count);
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    for i in 0..user_count {
        users.push((now as u128) << 80 | (7u128 << 76) | (i as u128));
    }

    let mut data = Vec::with_capacity(user_count * posts_per_user);
    let mut post_counter = 0u128;
    for &user_id in &users {
        for _ in 0..posts_per_user {
            let mut row = vec![0u128; dimension];
            row[0] = (now as u128) << 80 | (7u128 << 76) | post_counter; // post_id
            row[1] = user_id; // user_id
            data.push(row);
            post_counter += 1;
        }
    }
    (users, data)
}

fn bench_comparison(c: &mut Criterion) {
    let _guard = CleanupGuard;
    let rt = Runtime::new().unwrap();

    // 設定値
    let total_records = 1_000_000; // 100万件 (1ファイル約32MB)
    let dimension = 2;
    let user_count = 1000;
    let posts_per_user = 1000; // 計100万件
    let following_count = 100;
    let limit = 1000;

    // 計算
    let actual_data_size = user_count * posts_per_user;
    let mem_size_mb = (actual_data_size * dimension * 16) / 1024 / 1024;

    println!("\n{}", "=".repeat(40));
    println!("🚀 Starting Orby High-Load Benchmark");
    println!("{}", "=".repeat(40));
    println!("  - Total Capacity  : {} records", total_records);
    println!("  - Injection Data  : {} records", actual_data_size);
    println!("  - Dimension       : {}", dimension);
    println!("  - Estimated Data  : {} MB", mem_size_mb);
    println!(
        "  - Search Target   : {}/{} users",
        following_count, user_count
    );
    println!("  - Result Limit    : {}", limit);
    println!("{}", "=".repeat(40));
    println!("⏳ Generating dummy data and initializing engines...\n");

    let (users, data) = generate_dummy_data(user_count, posts_per_user, dimension);
    let following_ids: HashSet<u128> = users.iter().take(following_count).cloned().collect();

    // Prepare paths
    let sync_ring_path = PathBuf::from("db_data/bench_sync_ring.orby");
    let direct_ring_path = PathBuf::from("db_data/bench_direct_ring.orby");
    let sync_fixed_path = PathBuf::from("db_data/bench_sync_fixed.orby");
    let direct_fixed_path = PathBuf::from("db_data/bench_direct_fixed.orby");

    // Setup Ring Engines
    let engine_mem_ring = rt.block_on(async {
        let obs = Orby::new(
            "ring_mem",
            total_records,
            dimension,
            SaveMode::MemoryOnly,
            LogicMode::Ring,
        )
        .await
        .unwrap();
        obs.insert_batch(&data).await.unwrap();
        obs
    });
    let engine_sync_ring = rt.block_on(async {
        let obs = Orby::new(
            "ring_sync",
            total_records,
            dimension,
            SaveMode::Sync(Some(sync_ring_path.clone())),
            LogicMode::Ring,
        )
        .await
        .unwrap();
        obs.insert_batch(&data).await.unwrap();
        obs
    });
    let engine_direct_ring = rt.block_on(async {
        let obs = Orby::new(
            "ring_direct",
            total_records,
            dimension,
            SaveMode::Direct(Some(direct_ring_path.clone())),
            LogicMode::Ring,
        )
        .await
        .unwrap();
        obs.insert_batch(&data).await.unwrap();
        obs
    });

    // Setup Fixed Engines
    let engine_mem_fixed = rt.block_on(async {
        let obs = Orby::new(
            "fixed_mem",
            total_records,
            dimension,
            SaveMode::MemoryOnly,
            LogicMode::Fixed,
        )
        .await
        .unwrap();
        obs.insert_batch(&data).await.unwrap();
        obs
    });
    let engine_sync_fixed = rt.block_on(async {
        let obs = Orby::new(
            "fixed_sync",
            total_records,
            dimension,
            SaveMode::Sync(Some(sync_fixed_path.clone())),
            LogicMode::Fixed,
        )
        .await
        .unwrap();
        obs.insert_batch(&data).await.unwrap();
        obs
    });
    let engine_direct_fixed = rt.block_on(async {
        let obs = Orby::new(
            "fixed_direct",
            total_records,
            dimension,
            SaveMode::Direct(Some(direct_fixed_path.clone())),
            LogicMode::Fixed,
        )
        .await
        .unwrap();
        obs.insert_batch(&data).await.unwrap();
        obs
    });

    let mut group = c.benchmark_group("Orby Performance Comparison");

    // 01. Rust Standard
    group.bench_function("01_Rust_Standard_HashSet_Iter", |b| {
        b.iter(|| {
            let mut res = Vec::new();
            for item in data.iter().rev() {
                if following_ids.contains(&item[1]) {
                    res.push(item.clone());
                    if res.len() >= limit {
                        break;
                    }
                }
            }
            black_box(res);
        })
    });

    // Ring Logic
    group.bench_function("02_Orby_Ring_MemoryOnly", |b| {
        b.iter(|| {
            let res =
                engine_mem_ring.query_raw(|row| following_ids.contains(&row[1].as_u128()), limit);
            black_box(res);
        })
    });
    group.bench_function("03_Orby_Ring_Sync", |b| {
        b.iter(|| {
            let res =
                engine_sync_ring.query_raw(|row| following_ids.contains(&row[1].as_u128()), limit);
            black_box(res);
        })
    });
    group.bench_function("04_Orby_Ring_Direct", |b| {
        b.iter(|| {
            let res = engine_direct_ring
                .query_raw(|row| following_ids.contains(&row[1].as_u128()), limit);
            black_box(res);
        })
    });

    // Fixed Logic
    group.bench_function("05_Orby_Fixed_MemoryOnly", |b| {
        b.iter(|| {
            let res =
                engine_mem_fixed.query_raw(|row| following_ids.contains(&row[1].as_u128()), limit);
            black_box(res);
        })
    });
    group.bench_function("06_Orby_Fixed_Sync", |b| {
        b.iter(|| {
            let res =
                engine_sync_fixed.query_raw(|row| following_ids.contains(&row[1].as_u128()), limit);
            black_box(res);
        })
    });
    group.bench_function("07_Orby_Fixed_Direct", |b| {
        b.iter(|| {
            let res = engine_direct_fixed
                .query_raw(|row| following_ids.contains(&row[1].as_u128()), limit);
            black_box(res);
        })
    });

    group.finish();
}

criterion_group!(benches, bench_comparison);
criterion_main!(benches);
