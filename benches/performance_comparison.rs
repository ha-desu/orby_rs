use criterion::{black_box, criterion_group, criterion_main, Criterion};
use orby::{LogicMode, Orby, SaveMode};
use std::collections::HashSet;
use std::path::PathBuf;
use tokio::runtime::Runtime;

/// 実行終了時に一時ファイルを確実に削除するためのガード
struct CleanupGuard {
    paths: Vec<PathBuf>,
}

impl CleanupGuard {
    fn new() -> Self {
        Self { paths: Vec::new() }
    }
    fn add(&mut self, path: PathBuf) {
        self.paths.push(path);
    }
}

impl Drop for CleanupGuard {
    fn drop(&mut self) {
        for path in &self.paths {
            if path.exists() {
                let _ = std::fs::remove_dir_all(path);
            }
        }
    }
}

/// データの分布をより現実的に（インターリーブ）生成する
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

    let total_size = user_count * posts_per_user;
    let mut data = Vec::with_capacity(total_size);
    let mut post_counter = 0u128;

    for _ in 0..posts_per_user {
        for u in 0..user_count {
            let user_id = users[u];
            let mut row = vec![0u128; dimension];
            if dimension == 1 {
                // Dim=1の場合、フィルター対象(user_id)を0番目に入れる
                row[0] = user_id;
            } else {
                row[0] = (now as u128) << 80 | (7u128 << 76) | post_counter; // post_id
                row[1] = user_id; // user_id
                                  // 残りの次元はダミーデータ
                for d in 2..dimension {
                    row[d] = post_counter + d as u128;
                }
            }
            data.push(row);
            post_counter += 1;
        }
    }
    (users, data)
}

fn bench_comparison(c: &mut Criterion) {
    let mut guard = CleanupGuard::new();
    let rt = Runtime::new().unwrap();

    // --- 基本設定 ---
    let total_records = 1_000_000; // 次元を増やすため、メモリ考慮で100万件に調整（比較には十分）
    let user_count = 10_000;
    let posts_per_user = 100;
    let following_count = 500;
    let limit = 100;

    println!("\n{}", "=".repeat(50));
    println!("🔥 Orby v0.2.0 Multi-Dimension Benchmark");
    println!("{}", "=".repeat(50));

    // ---------------------------------------------------------
    // Case A: Dimension 1 (Baseline vs Orby)
    // ---------------------------------------------------------
    {
        let dim = 1;
        let (users, data) = generate_dummy_data(user_count, posts_per_user, dim);
        let step = user_count / following_count;
        let following_ids: HashSet<u128> = users
            .iter()
            .step_by(step)
            .take(following_count)
            .cloned()
            .collect();

        let engine = rt.block_on(async {
            let obs = Orby::new(
                "bench_dim1",
                total_records,
                dim,
                SaveMode::MemoryOnly,
                LogicMode::RingBuffer,
            )
            .await
            .unwrap();
            obs.insert_batch(&data).await.unwrap();
            obs
        });

        let mut group = c.benchmark_group("Orby vs Baseline (Dimension=1)");
        group.bench_function("01_Rust_Standard_Dim1", |b| {
            b.iter(|| {
                let mut res = Vec::new();
                for item in data.iter().rev() {
                    if following_ids.contains(&item[0]) {
                        res.push(item.clone());
                        if res.len() >= limit {
                            break;
                        }
                    }
                }
                black_box(res);
            })
        });
        group.bench_function("02_Orby_RingBuffer_Dim1", |b| {
            b.iter(|| {
                let res = engine.query_raw(|row| following_ids.contains(&row[0].as_u128()), limit);
                black_box(res);
            })
        });
        group.finish();
    }

    // ---------------------------------------------------------
    // Case B: Dimension Scaling (2, 4, 8, 12)
    // ---------------------------------------------------------
    let dimensions = vec![2, 4, 8, 12];
    let mut group = c.benchmark_group("Orby Dimension Scaling");

    for dim in dimensions {
        let (users, data) = generate_dummy_data(user_count, posts_per_user, dim);
        let step = user_count / following_count;
        let following_ids: HashSet<u128> = users
            .iter()
            .step_by(step)
            .take(following_count)
            .cloned()
            .collect();

        let engine = rt.block_on(async {
            let obs = Orby::new(
                &format!("bench_dim{}", dim),
                total_records,
                dim,
                SaveMode::MemoryOnly,
                LogicMode::RingBuffer,
            )
            .await
            .unwrap();
            obs.insert_batch(&data).await.unwrap();
            obs
        });

        group.bench_function(format!("Orby_Dim_{:02}", dim), |b| {
            b.iter(|| {
                let res = engine.query_raw(|row| following_ids.contains(&row[1].as_u128()), limit);
                black_box(res);
            })
        });
    }
    group.finish();

    // ---------------------------------------------------------
    // Case C: Vault vs Memory (Dim=2)
    // ---------------------------------------------------------
    {
        let dim = 2;
        let (users, data) = generate_dummy_data(user_count, posts_per_user, dim);
        let step = user_count / following_count;
        let following_ids: HashSet<u128> = users
            .iter()
            .step_by(step)
            .take(following_count)
            .cloned()
            .collect();

        let engine_mem = rt.block_on(async {
            let obs = Orby::new(
                "bench_mem_v2",
                total_records,
                dim,
                SaveMode::MemoryOnly,
                LogicMode::RingBuffer,
            )
            .await
            .unwrap();
            obs.insert_batch(&data).await.unwrap();
            obs
        });

        let vault_path = PathBuf::from("bench_vault_scaling");
        guard.add(vault_path.clone());
        let engine_vault = rt.block_on(async {
            if vault_path.exists() {
                let _ = std::fs::remove_dir_all(&vault_path);
            }
            let obs = Orby::new(
                "bench_vault_v2",
                total_records,
                dim,
                SaveMode::Vault(Some(vault_path)),
                LogicMode::RingBuffer,
            )
            .await
            .unwrap();
            obs.insert_batch(&data).await.unwrap();
            obs
        });

        let mut group = c.benchmark_group("Orby Storage Overhead (Dim=2)");
        group.bench_function("01_MemoryOnly", |b| {
            b.iter(|| {
                let res =
                    engine_mem.query_raw(|row| following_ids.contains(&row[1].as_u128()), limit);
                black_box(res);
            })
        });
        group.bench_function("02_Vault", |b| {
            b.iter(|| {
                let res =
                    engine_vault.query_raw(|row| following_ids.contains(&row[1].as_u128()), limit);
                black_box(res);
            })
        });
        group.finish();
    }

    println!("\n✅ All dimensions benchmarked. Cleaning up...");
}

criterion_group!(benches, bench_comparison);
criterion_main!(benches);
