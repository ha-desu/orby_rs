use crate::types::OrbitField;

/// 固定された次元数 `N` を持つ、キャッシュアラインメント済みの行データ構造体です。
/// パフォーマンスが最優先される場合に適しています。
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct OrbyRow<const N: usize> {
    pub values: [OrbitField; N],
}

impl<const N: usize> OrbyRow<N> {
    /// 新しい固定次元の行データを生成します。
    pub fn new(values: [u128; N]) -> Self {
        Self {
            values: values.map(OrbitField::new),
        }
    }

    /// OrbitField 形式で新しい行データを生成します。
    pub fn from_fields(values: [OrbitField; N]) -> Self {
        Self { values }
    }
}

impl<const N: usize> From<[u128; N]> for OrbyRow<N> {
    fn from(vals: [u128; N]) -> Self {
        Self::new(vals)
    }
}

impl<const N: usize> From<[OrbitField; N]> for OrbyRow<N> {
    fn from(values: [OrbitField; N]) -> Self {
        Self { values }
    }
}

impl<const N: usize> From<OrbyRow<N>> for [u128; N] {
    fn from(row: OrbyRow<N>) -> Self {
        row.values.map(|b| b.as_u128())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{align_of, size_of};

    #[test]
    fn test_row_size_and_alignment() {
        // Dimension 1: 16bytes -> aligned to 64 bytes
        assert_eq!(size_of::<OrbyRow<1>>(), 64);
        assert_eq!(align_of::<OrbyRow<1>>(), 64);

        // Dimension 2: 32bytes -> aligned to 64 bytes
        assert_eq!(size_of::<OrbyRow<2>>(), 64);
        assert_eq!(align_of::<OrbyRow<2>>(), 64);

        // Dimension 4: 64bytes -> aligned to 64 bytes
        assert_eq!(size_of::<OrbyRow<4>>(), 64);

        // Dimension 5: 80bytes -> aligned to 128 bytes
        assert_eq!(size_of::<OrbyRow<5>>(), 128);
    }
}
