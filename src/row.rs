use crate::types::PulseCell;

/// 固定された次元数 `N` を持つ、キャッシュアラインメント済みの行データ構造体です。
/// パフォーマンスが最優先される場合に適しています。
#[repr(C, align(64))]
#[derive(Debug, Clone, Copy)]
pub struct PulseCellPack<const N: usize> {
    pub values: [PulseCell; N],
}

impl<const N: usize> PulseCellPack<N> {
    /// 新しい固定次元の行データを生成します。
    pub fn new(values: [u128; N]) -> Self {
        Self {
            values: values.map(PulseCell::new),
        }
    }

    /// PulseCell 形式で新しい行データを生成します。
    pub fn from_fields(values: [PulseCell; N]) -> Self {
        Self { values }
    }
}

impl<const N: usize> From<[u128; N]> for PulseCellPack<N> {
    fn from(vals: [u128; N]) -> Self {
        Self::new(vals)
    }
}

impl<const N: usize> From<[PulseCell; N]> for PulseCellPack<N> {
    fn from(values: [PulseCell; N]) -> Self {
        Self { values }
    }
}

impl<const N: usize> From<PulseCellPack<N>> for [u128; N] {
    fn from(row: PulseCellPack<N>) -> Self {
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
        assert_eq!(size_of::<PulseCellPack<1>>(), 64);
        assert_eq!(align_of::<PulseCellPack<1>>(), 64);

        // Dimension 2: 32bytes -> aligned to 64 bytes
        assert_eq!(size_of::<PulseCellPack<2>>(), 64);
        assert_eq!(align_of::<PulseCellPack<2>>(), 64);

        // Dimension 4: 64bytes -> aligned to 64 bytes
        assert_eq!(size_of::<PulseCellPack<4>>(), 64);

        // Dimension 5: 80bytes -> aligned to 128 bytes
        assert_eq!(size_of::<PulseCellPack<5>>(), 128);
    }
}
