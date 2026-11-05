//! Small SQL helpers shared by both dialects.

/// Most values bound in one `IN (...)` list or multi-row `VALUES`. Well
/// under SQLite's default limit of 32,766 parameters and Postgres's 65,535.
pub(crate) const CHUNK: usize = 500;

/// `$first, $first+1, ...` for `n` parameters, for an `IN (...)` list.
/// Both dialects accept `$n` placeholders through sqlx.
pub(crate) fn placeholders(first: usize, n: usize) -> String {
    (first..first + n)
        .map(|i| format!("${i}"))
        .collect::<Vec<_>>()
        .join(", ")
}

/// The smallest string greater than every string that starts with
/// `prefix`, in bytewise (code point) order, or `None` if there is none.
///
/// Ref names sort bytewise (`COLLATE "C"` in Postgres, `BINARY` in SQLite),
/// and UTF-8 byte order is code point order, so "starts with `prefix`" is
/// exactly `prefix <= name < upper`. That range uses the `refs` primary key,
/// where `LIKE` would need escaping and is case-insensitive in SQLite.
pub(crate) fn prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars: Vec<char> = prefix.chars().collect();
    while let Some(last) = chars.pop() {
        let next = match last {
            '\u{D7FF}' => Some('\u{E000}'),
            c => char::from_u32(u32::from(c) + 1),
        };
        if let Some(next) = next {
            chars.push(next);
            return Some(chars.into_iter().collect());
        }
        // `last` is U+10FFFF: nothing follows it, so carry into the
        // previous character.
    }
    None
}

/// Most prefix ranges in one ref listing statement. Each binds two values,
/// so a statement binds at most [`CHUNK`] of them, and its `OR` chain stays
/// far below SQLite's expression depth limit of 1,000, which a chain of
/// about 1,000 ranges reaches.
pub(crate) const RANGES_PER_QUERY: usize = CHUNK / 2;

/// The names that start with one prefix: `lo <= name < hi`, with no upper
/// end when `hi` is `None` (see [`prefix_upper_bound`]).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PrefixRange {
    pub(crate) lo: String,
    pub(crate) hi: Option<String>,
}

impl PrefixRange {
    /// Whether every name in the range sorts at or before `name`.
    pub(crate) fn ends_by(&self, name: &str) -> bool {
        self.hi.as_deref().is_some_and(|hi| hi <= name)
    }
}

/// The ranges a listing filtered by `prefixes` reads: one per prefix that
/// no other prefix in the list covers (a prefix covers every name that
/// starts with it, so also every longer prefix that does, and a repeated
/// prefix covers its copies). They are disjoint and in name order. `None`
/// when every name matches: no prefixes at all, or an empty one.
///
/// So a request's cost follows the distinct name ranges it asks for, not
/// the number of lines it sends (#119).
pub(crate) fn prefix_ranges(prefixes: &[String]) -> Option<Vec<PrefixRange>> {
    if prefixes.is_empty() || prefixes.iter().any(String::is_empty) {
        return None;
    }
    let mut sorted: Vec<&str> = prefixes.iter().map(String::as_str).collect();
    sorted.sort_unstable();
    let mut kept: Vec<&str> = Vec::new();
    for p in sorted {
        // In sorted order a prefix comes before every name that starts
        // with it, and so does everything between them; so if anything
        // kept covers `p`, the last one kept does.
        if kept.last().is_none_or(|k| !p.starts_with(k)) {
            kept.push(p);
        }
    }
    Some(
        kept.into_iter()
            .map(|p| PrefixRange {
                lo: p.to_owned(),
                hi: prefix_upper_bound(p),
            })
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn placeholders_number_from_first() {
        assert_eq!(placeholders(3, 3), "$3, $4, $5");
        assert_eq!(placeholders(1, 0), "");
    }

    #[test]
    fn the_upper_bound_follows_every_name_with_the_prefix_and_nothing_else() {
        assert_eq!(
            prefix_upper_bound("refs/heads/").as_deref(),
            Some("refs/heads0")
        );
        assert_eq!(
            prefix_upper_bound("a\u{D7FF}").as_deref(),
            Some("a\u{E000}")
        );
        assert_eq!(prefix_upper_bound("a\u{10FFFF}").as_deref(), Some("b"));
        assert_eq!(prefix_upper_bound("\u{10FFFF}"), None);
        assert_eq!(prefix_upper_bound(""), None);
        // Byte order agrees with the bound for multi-byte characters.
        let upper = prefix_upper_bound("refs/tags/é").unwrap();
        for name in ["refs/tags/é", "refs/tags/é\u{10FFFF}x", "refs/tags/éz"] {
            assert!(name.as_bytes() < upper.as_bytes(), "{name}");
            assert!(name.as_bytes() >= "refs/tags/é".as_bytes());
        }
        assert!("refs/tags/ê".as_bytes() >= upper.as_bytes());
    }

    fn los(prefixes: &[&str]) -> Option<Vec<String>> {
        let prefixes: Vec<String> = prefixes.iter().map(|p| (*p).to_owned()).collect();
        prefix_ranges(&prefixes).map(|r| r.into_iter().map(|r| r.lo).collect())
    }

    /// Invariant: the ranges are exactly the prefixes no other prefix
    /// covers, sorted, so they are disjoint and in name order.
    #[test]
    fn prefix_ranges_keep_only_the_prefixes_nothing_else_covers() {
        assert_eq!(los(&[]), None);
        assert_eq!(los(&["refs/heads/", ""]), None);
        assert_eq!(
            los(&["refs/tags/", "refs/heads/a", "refs/heads/a", "refs/heads/"]),
            Some(vec!["refs/heads/".into(), "refs/tags/".into()])
        );
        // `a` covers `ab` and `ac` although `ab` sorts between them, and
        // `b` is not covered by `a`'s range.
        assert_eq!(
            los(&["ac", "b", "ab", "a", "a/"]),
            Some(vec!["a".into(), "b".into()])
        );
        let ranges = prefix_ranges(&["x".into(), "refs/".into()]).unwrap();
        assert!(ranges.windows(2).all(|w| w[0].ends_by(&w[1].lo)));
        assert_eq!(ranges[0].hi.as_deref(), Some("refs0"));
        assert!(ranges[0].ends_by("refs0"));
        assert!(!ranges[0].ends_by("refs/zzz"));
        let open = prefix_ranges(&["\u{10FFFF}".into()]).unwrap();
        assert!(!open[0].ends_by("\u{10FFFF}\u{10FFFF}"));
    }
}
