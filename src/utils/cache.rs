use once_cell::sync::Lazy;

/// 並列処理の最適化に使用される L3 キャッシュサイズ。
/// L3 が取得できない場合は L2、それでも不明な場合は 16MB をデフォルトとします。
pub(crate) static L3_CACHE_SIZE: Lazy<usize> = Lazy::new(|| {
    cache_size::l3_cache_size()
        .or_else(cache_size::l2_cache_size)
        .unwrap_or(16 * 1024 * 1024)
});

/// L3 キャッシュサイズに基づいて、最適な並列分割単位（min_len）を計算します。
pub(crate) fn calculate_min_len(padded_dim: usize) -> usize {
    let cpus = rayon::current_num_threads();
    let l3_cache_size = *L3_CACHE_SIZE;

    // 実際にメモリ上で1行が占めるサイズ (padded_dim * 16 bytes)
    let row_memory_size = 16 * padded_dim;
    let max_rows_in_cache = l3_cache_size / row_memory_size;

    // 各スレッドがキャッシュを食い潰さず、かつ効率的に動ける行数を算出
    // 下限 512, 上限 8192 あたりが、現代の CPU (x86_64/AArch64) では一般的です
    (max_rows_in_cache / cpus).clamp(512, 8192)
}
