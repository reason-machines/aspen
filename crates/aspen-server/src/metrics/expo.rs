//! A small writer for the Prometheus text exposition format, version
//! 0.0.4 (<https://prometheus.io/docs/instrumenting/exposition_formats/>).
//!
//! Every family is written in one piece: `# HELP`, `# TYPE`, then its
//! samples, so a family can never be split or repeated by accident. Label
//! values are escaped as the format requires (`\`, `"` and newlines).
//! Names are checked against the format's grammar in debug builds and by
//! the tests that render the whole endpoint.

use std::fmt::Write;
use std::sync::atomic::{AtomicU64, Ordering};

/// The `Content-Type` of the exposition.
pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// A metric family's type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Counter,
    Gauge,
}

impl Kind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Counter => "counter",
            Self::Gauge => "gauge",
        }
    }
}

/// Whether `name` is a valid metric or label name
/// (`[a-zA-Z_:][a-zA-Z0-9_:]*`; label names may not contain `:`).
pub(crate) fn valid_name(name: &str, label: bool) -> bool {
    let mut chars = name.chars();
    let ok_first = |c: char| c.is_ascii_alphabetic() || c == '_' || (!label && c == ':');
    let ok_rest = |c: char| ok_first(c) || c.is_ascii_digit();
    chars.next().is_some_and(ok_first) && chars.all(ok_rest)
}

fn escape_label(value: &str, out: &mut String) {
    for c in value.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

fn escape_help(help: &str, out: &mut String) {
    for c in help.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
}

/// A number as the format writes it: integers without a fraction, and
/// `+Inf`, `-Inf` and `NaN` spelled out.
fn number(v: f64, out: &mut String) {
    if v.is_nan() {
        out.push_str("NaN");
    } else if v.is_infinite() {
        out.push_str(if v > 0.0 { "+Inf" } else { "-Inf" });
    } else {
        let _ = write!(out, "{v}");
    }
}

fn header(out: &mut String, name: &str, help: &str, kind: &str) {
    debug_assert!(valid_name(name, false), "metric name {name:?}");
    let _ = write!(out, "# HELP {name} ");
    escape_help(help, out);
    let _ = writeln!(out, "\n# TYPE {name} {kind}");
}

fn sample(out: &mut String, name: &str, labels: &[(&str, &str)], v: f64) {
    out.push_str(name);
    if !labels.is_empty() {
        out.push('{');
        for (i, (k, val)) in labels.iter().enumerate() {
            debug_assert!(valid_name(k, true), "label name {k:?}");
            if i > 0 {
                out.push(',');
            }
            let _ = write!(out, "{k}=\"");
            escape_label(val, out);
            out.push('"');
        }
        out.push('}');
    }
    out.push(' ');
    number(v, out);
    out.push('\n');
}

/// Writes a family with one unlabelled sample.
pub(crate) fn single(out: &mut String, name: &str, help: &str, kind: Kind, v: f64) {
    header(out, name, help, kind.as_str());
    sample(out, name, &[], v);
}

/// Writes a counter with one unlabelled sample.
pub(crate) fn counter(out: &mut String, name: &str, help: &str, v: u64) {
    single(out, name, help, Kind::Counter, v as f64);
}

/// Writes a gauge with one unlabelled sample.
pub(crate) fn gauge(out: &mut String, name: &str, help: &str, v: f64) {
    single(out, name, help, Kind::Gauge, v);
}

/// Writes a family whose samples differ by labels. Every sample must carry
/// the same label names; an empty iterator still writes the header, so the
/// family is always present.
pub(crate) fn family<'a>(
    out: &mut String,
    name: &str,
    help: &str,
    kind: Kind,
    samples: impl IntoIterator<Item = (Vec<(&'a str, &'a str)>, f64)>,
) {
    header(out, name, help, kind.as_str());
    for (labels, v) in samples {
        sample(out, name, &labels, v);
    }
}

/// A histogram over non-negative integers (a count, or a duration in
/// microseconds), rendered in the recorded unit divided by `divisor` (1
/// for counts, [`MICROS_PER_SECOND`] to render microseconds as seconds).
/// Dividing, rather than multiplying by a fraction, renders 1,000 us as
/// exactly `0.001`.
#[derive(Debug)]
pub(crate) struct Histogram {
    /// Upper bounds of the buckets, increasing, in the recorded unit.
    bounds: &'static [u64],
    /// Observations per bucket (not cumulative); the last is `+Inf`.
    buckets: Box<[AtomicU64]>,
    sum: AtomicU64,
    count: AtomicU64,
    divisor: f64,
}

impl Histogram {
    pub(crate) fn new(bounds: &'static [u64], divisor: f64) -> Self {
        debug_assert!(bounds.windows(2).all(|w| w[0] < w[1]));
        Self {
            bounds,
            buckets: (0..=bounds.len()).map(|_| AtomicU64::new(0)).collect(),
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
            divisor,
        }
    }

    pub(crate) fn observe(&self, v: u64) {
        let i = self
            .bounds
            .iter()
            .position(|b| v <= *b)
            .unwrap_or(self.bounds.len());
        self.buckets[i].fetch_add(1, Ordering::Relaxed);
        self.sum.fetch_add(v, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    /// Observations so far.
    pub(crate) fn count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Sum of the observations, in the recorded unit.
    pub(crate) fn sum(&self) -> u64 {
        self.sum.load(Ordering::Relaxed)
    }

    /// Writes the family: cumulative `_bucket` samples ending in `+Inf`,
    /// then `_sum` and `_count`. The counts are read once, so `+Inf`
    /// always equals `_count` in one scrape even while requests record.
    pub(crate) fn render(&self, out: &mut String, name: &str, help: &str) {
        header(out, name, help, "histogram");
        let counts: Vec<u64> = self
            .buckets
            .iter()
            .map(|b| b.load(Ordering::Relaxed))
            .collect();
        let total: u64 = counts.iter().sum();
        let bucket = format!("{name}_bucket");
        let mut cumulative = 0;
        for (b, n) in self.bounds.iter().zip(&counts) {
            cumulative += n;
            let mut le = String::new();
            number(*b as f64 / self.divisor, &mut le);
            sample(out, &bucket, &[("le", &le)], cumulative as f64);
        }
        sample(out, &bucket, &[("le", "+Inf")], total as f64);
        sample(
            out,
            &format!("{name}_sum"),
            &[],
            self.sum.load(Ordering::Relaxed) as f64 / self.divisor,
        );
        sample(out, &format!("{name}_count"), &[], total as f64);
    }
}

/// Bucket bounds for request and stage durations, in microseconds: 1 ms to
/// 10 minutes.
pub(crate) const DURATION_MICROS: [u64; 16] = [
    1_000,
    5_000,
    10_000,
    25_000,
    50_000,
    100_000,
    250_000,
    500_000,
    1_000_000,
    2_500_000,
    5_000_000,
    10_000_000,
    30_000_000,
    60_000_000,
    180_000_000,
    600_000_000,
];

/// Converts microseconds to the histograms' recorded unit.
pub(crate) fn micros(d: std::time::Duration) -> u64 {
    u64::try_from(d.as_micros()).unwrap_or(u64::MAX)
}

/// The divisor of duration histograms, which record microseconds.
pub(crate) const MICROS_PER_SECOND: f64 = 1e6;

#[cfg(test)]
mod tests {
    use super::*;

    /// Label values are escaped, so a value cannot end its label or start
    /// a new sample.
    #[test]
    fn label_values_are_escaped() {
        let mut s = String::new();
        family(
            &mut s,
            "x_total",
            "a \\ help\nline",
            Kind::Counter,
            [(vec![("reason", "a\"b\\c\nd")], 1.0)],
        );
        assert_eq!(
            s,
            "# HELP x_total a \\\\ help\\nline\n# TYPE x_total counter\nx_total{reason=\"a\\\"b\\\\c\\nd\"} 1\n"
        );
    }

    /// Buckets are cumulative, end with `+Inf` equal to `_count`, and the
    /// bounds and sum are converted into the rendered unit.
    #[test]
    fn histograms_are_cumulative_and_scaled() {
        static B: [u64; 2] = [1_000, 1_000_000];
        let h = Histogram::new(&B, MICROS_PER_SECOND);
        for v in [10, 1_000, 5_000, 2_000_000] {
            h.observe(v);
        }
        let mut s = String::new();
        h.render(&mut s, "d_seconds", "d.");
        assert_eq!(
            s,
            "# HELP d_seconds d.\n# TYPE d_seconds histogram\n\
             d_seconds_bucket{le=\"0.001\"} 2\n\
             d_seconds_bucket{le=\"1\"} 3\n\
             d_seconds_bucket{le=\"+Inf\"} 4\n\
             d_seconds_sum 2.00601\n\
             d_seconds_count 4\n"
        );
    }

    #[test]
    fn names_follow_the_grammar() {
        assert!(valid_name("aspen_jobs", false));
        assert!(valid_name("a:b", false));
        assert!(!valid_name("a:b", true));
        assert!(!valid_name("1a", false));
        assert!(!valid_name("", false));
        assert!(!valid_name("a-b", false));
    }
}
