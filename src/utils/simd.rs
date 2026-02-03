/// SIMD Optimization Helpers
/// 特定の単一ターゲットを高速に検索する場合、wide クレートを使用して 2 つの u128 (OrbitField) を
/// 同時に比較することが可能です。

#[inline(always)]
pub fn match_u128_x2(vals: [u128; 2], target: u128) -> [bool; 2] {
    use wide::*;
    let t_low = u64x2::splat(target as u64);
    let t_high = u64x2::splat((target >> 64) as u64);
    let v_low = u64x2::from([vals[0] as u64, vals[1] as u64]);
    let v_high = u64x2::from([(vals[0] >> 64) as u64, (vals[1] >> 64) as u64]);
    let res = v_low.cmp_eq(t_low) & v_high.cmp_eq(t_high);
    let mask: [u64; 2] = res.into();
    [mask[0] != 0, mask[1] != 0]
}
