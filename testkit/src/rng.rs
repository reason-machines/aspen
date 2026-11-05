//! A tiny seeded random number generator.
//!
//! The oracle's sequences must replay identically from a printed seed on any
//! machine and after any dependency upgrade, so the generator is defined here
//! (SplitMix64, public domain, Steele, Lea and Flood 2014) instead of borrowed
//! from a crate whose output may change between versions.

/// SplitMix64. Not cryptographic; only for reproducible test inputs.
#[derive(Debug, Clone)]
pub struct Rng(u64);

impl Rng {
    /// A generator that always yields the same values for the same seed.
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A value in `0..n`. `n` must be non-zero. The modulo bias is
    /// irrelevant for test generation and keeps the mapping obvious.
    pub fn below(&mut self, n: usize) -> usize {
        assert!(n > 0, "Rng::below(0)");
        (self.next_u64() % n as u64) as usize
    }

    /// Picks an index into `weights` with probability proportional to its
    /// weight. The weights must not all be zero.
    pub fn weighted(&mut self, weights: &[u32]) -> usize {
        let total: u64 = weights.iter().map(|&w| u64::from(w)).sum();
        assert!(total > 0, "Rng::weighted with all-zero weights");
        let mut pick = self.next_u64() % total;
        for (i, &w) in weights.iter().enumerate() {
            if pick < u64::from(w) {
                return i;
            }
            pick -= u64::from(w);
        }
        unreachable!("pick is below the total weight")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn output_is_pinned() {
        // Reference values for SplitMix64 seeded with 0. If this test fails,
        // every printed seed from earlier runs stops reproducing.
        let mut rng = Rng::new(0);
        assert_eq!(rng.next_u64(), 0xe220_a839_7b1d_cdaf);
        assert_eq!(rng.next_u64(), 0x6e78_9e6a_a1b9_65f4);
        assert_eq!(rng.next_u64(), 0x06c4_5d18_8009_454f);
    }

    #[test]
    fn weighted_never_picks_zero_weights() {
        let mut rng = Rng::new(7);
        for _ in 0..1000 {
            assert_ne!(rng.weighted(&[3, 0, 5]), 1);
        }
    }
}
