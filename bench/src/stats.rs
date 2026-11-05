//! Latency statistics.
//!
//! Percentiles use the nearest-rank method: the p-th percentile of `n` sorted
//! samples is the sample at 1-based rank `ceil(p / 100 * n)`. It always
//! returns a value that was actually measured, which keeps small runs honest
//! (no interpolated latency that no request ever had).

use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Summary of a set of latency samples, in milliseconds.
///
/// Every field except `count` is `None` when there were no samples, so an
/// empty run never reports a fake zero latency.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Summary {
    pub count: u64,
    pub min: Option<f64>,
    pub mean: Option<f64>,
    pub p50: Option<f64>,
    pub p90: Option<f64>,
    pub p99: Option<f64>,
    pub max: Option<f64>,
}

impl Summary {
    /// Summarises samples given in milliseconds. Order does not matter.
    pub fn from_millis(samples: &[f64]) -> Self {
        let mut sorted: Vec<f64> = samples.iter().copied().filter(|s| s.is_finite()).collect();
        sorted.sort_by(f64::total_cmp);
        if sorted.is_empty() {
            return Self {
                count: 0,
                min: None,
                mean: None,
                p50: None,
                p90: None,
                p99: None,
                max: None,
            };
        }
        let mean = sorted.iter().sum::<f64>() / sorted.len() as f64;
        Self {
            count: sorted.len() as u64,
            min: sorted.first().copied(),
            mean: Some(mean),
            p50: percentile_sorted(&sorted, 50.0),
            p90: percentile_sorted(&sorted, 90.0),
            p99: percentile_sorted(&sorted, 99.0),
            max: sorted.last().copied(),
        }
    }

    /// Summarises durations.
    pub fn from_durations(samples: &[Duration]) -> Self {
        let millis: Vec<f64> = samples.iter().map(|d| millis(*d)).collect();
        Self::from_millis(&millis)
    }

    /// The summary as `(field, value)` pairs, for flattening into metric keys.
    pub fn fields(&self) -> [(&'static str, Option<f64>); 7] {
        [
            ("count", Some(self.count as f64)),
            ("min", self.min),
            ("mean", self.mean),
            ("p50", self.p50),
            ("p90", self.p90),
            ("p99", self.p99),
            ("max", self.max),
        ]
    }
}

/// A duration in milliseconds, with sub-millisecond precision.
pub fn millis(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

/// Nearest-rank percentile of already sorted samples; `p` is in `(0, 100]`.
/// Returns `None` for no samples or a `p` outside that range.
pub fn percentile_sorted(sorted: &[f64], p: f64) -> Option<f64> {
    if sorted.is_empty() || !(p > 0.0 && p <= 100.0) {
        return None;
    }
    let n = sorted.len();
    // Clamp guards against floating-point rounding pushing the rank past n.
    // Multiply before dividing: `p * n` is exact for whole numbers, so ranks
    // such as 99% of 100 samples land on 99, not 100.
    let rank = (p * n as f64 / 100.0).ceil() as usize;
    sorted.get(rank.clamp(1, n) - 1).copied()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn nearest_rank_matches_the_textbook_example() {
        // Classic example: 15, 20, 35, 40, 50.
        let s = [15.0, 20.0, 35.0, 40.0, 50.0];
        assert_eq!(percentile_sorted(&s, 5.0), Some(15.0));
        assert_eq!(percentile_sorted(&s, 30.0), Some(20.0));
        assert_eq!(percentile_sorted(&s, 40.0), Some(20.0));
        assert_eq!(percentile_sorted(&s, 50.0), Some(35.0));
        assert_eq!(percentile_sorted(&s, 100.0), Some(50.0));
    }

    #[test]
    fn percentiles_over_one_to_a_hundred() {
        let s: Vec<f64> = (1..=100).map(f64::from).collect();
        assert_eq!(percentile_sorted(&s, 50.0), Some(50.0));
        assert_eq!(percentile_sorted(&s, 90.0), Some(90.0));
        assert_eq!(percentile_sorted(&s, 99.0), Some(99.0));
        assert_eq!(percentile_sorted(&s, 99.9), Some(100.0));
    }

    #[test]
    fn out_of_range_percentiles_and_empty_input_are_none() {
        assert_eq!(percentile_sorted(&[], 50.0), None);
        assert_eq!(percentile_sorted(&[1.0], 0.0), None);
        assert_eq!(percentile_sorted(&[1.0], 100.1), None);
        assert_eq!(percentile_sorted(&[1.0], f64::NAN), None);
    }

    #[test]
    fn single_sample_is_every_percentile() {
        let s = Summary::from_millis(&[7.5]);
        assert_eq!(s.count, 1);
        for v in [s.min, s.mean, s.p50, s.p90, s.p99, s.max] {
            assert_eq!(v, Some(7.5));
        }
    }

    #[test]
    fn summary_ignores_input_order_and_non_finite_samples() {
        let a = Summary::from_millis(&[3.0, 1.0, 2.0, f64::NAN, f64::INFINITY]);
        let b = Summary::from_millis(&[1.0, 2.0, 3.0]);
        assert_eq!(a, b);
        assert_eq!(a.count, 3);
        assert_eq!(a.mean, Some(2.0));
        assert_eq!(a.min, Some(1.0));
        assert_eq!(a.max, Some(3.0));
    }

    #[test]
    fn empty_summary_has_no_values() {
        let s = Summary::from_millis(&[]);
        assert_eq!(s.count, 0);
        assert!(s.p50.is_none() && s.mean.is_none() && s.max.is_none());
        // And it survives a JSON round trip as nulls.
        let json = serde_json::to_string(&s).unwrap();
        assert!(json.contains("\"p99\":null"), "{json}");
        assert_eq!(serde_json::from_str::<Summary>(&json).unwrap(), s);
    }

    #[test]
    fn p99_reflects_the_tail() {
        // 98 fast requests and 2 slow ones: p50 is fast, p99 is slow.
        let mut s = vec![10.0; 98];
        s.extend([500.0, 900.0]);
        let sum = Summary::from_millis(&s);
        assert_eq!(sum.p50, Some(10.0));
        assert_eq!(sum.p90, Some(10.0));
        assert_eq!(sum.p99, Some(500.0));
        assert_eq!(sum.max, Some(900.0));
    }

    #[test]
    fn durations_are_converted_to_millis() {
        let s = Summary::from_durations(&[Duration::from_micros(1500), Duration::from_secs(1)]);
        assert_eq!(s.min, Some(1.5));
        assert_eq!(s.max, Some(1000.0));
    }
}
