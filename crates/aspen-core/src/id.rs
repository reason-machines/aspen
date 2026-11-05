//! Identifiers: Git object IDs, pack checksums and database row IDs.
//!
//! Each kind of ID is its own type, so a [`PackId`] can never be passed where
//! a [`RefVersionId`] is expected, and an [`ObjectId`] can never be confused
//! with the [`PackChecksum`] of a pack file.

use std::fmt;
use std::str::FromStr;

/// Length of a SHA-1 digest in bytes.
pub const SHA1_LEN: usize = 20;

/// Length of a SHA-1 digest written as hexadecimal.
pub const SHA1_HEX_LEN: usize = 40;

/// Why a string or byte slice is not a valid ID.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IdError {
    /// The input has the wrong length.
    #[error("expected {expected} characters or bytes, found {found}")]
    WrongLength {
        /// Required length.
        expected: usize,
        /// Length of the input.
        found: usize,
    },
    /// A character is not a lowercase hexadecimal digit. Git always writes
    /// object IDs in lowercase, and Aspen accepts only that form so that
    /// every ID has exactly one spelling.
    #[error("character {position} is not a lowercase hexadecimal digit")]
    NotLowercaseHex {
        /// Zero-based position of the first bad character.
        position: usize,
    },
    /// A database row ID is zero or negative.
    #[error("row id {0} is not positive")]
    NotPositive(i64),
    /// A sequence number is outside `1..=i64::MAX`.
    #[error("sequence number {0} is outside 1..=i64::MAX")]
    SeqOutOfRange(i128),
}

fn parse_sha1_hex(s: &str) -> Result<[u8; SHA1_LEN], IdError> {
    let bytes = s.as_bytes();
    if bytes.len() != SHA1_HEX_LEN {
        return Err(IdError::WrongLength {
            expected: SHA1_HEX_LEN,
            found: bytes.len(),
        });
    }
    let nibble = |position: usize| -> Result<u8, IdError> {
        match bytes[position] {
            c @ b'0'..=b'9' => Ok(c - b'0'),
            c @ b'a'..=b'f' => Ok(c - b'a' + 10),
            _ => Err(IdError::NotLowercaseHex { position }),
        }
    };
    let mut out = [0u8; SHA1_LEN];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = (nibble(2 * i)? << 4) | nibble(2 * i + 1)?;
    }
    Ok(out)
}

fn write_hex(f: &mut fmt::Formatter<'_>, bytes: &[u8]) -> fmt::Result {
    for b in bytes {
        write!(f, "{b:02x}")?;
    }
    Ok(())
}

macro_rules! sha1_newtype {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name([u8; SHA1_LEN]);

        impl $name {
            /// Wraps raw digest bytes.
            pub const fn from_bytes(bytes: [u8; SHA1_LEN]) -> Self {
                Self(bytes)
            }

            /// Copies a 20-byte slice, as stored in a database `BYTEA`/`BLOB` column.
            pub fn from_slice(bytes: &[u8]) -> Result<Self, IdError> {
                let array: [u8; SHA1_LEN] =
                    bytes.try_into().map_err(|_| IdError::WrongLength {
                        expected: SHA1_LEN,
                        found: bytes.len(),
                    })?;
                Ok(Self(array))
            }

            /// Parses exactly 40 lowercase hexadecimal digits.
            pub fn parse_hex(s: &str) -> Result<Self, IdError> {
                parse_sha1_hex(s).map(Self)
            }

            /// The raw digest bytes.
            pub const fn as_bytes(&self) -> &[u8; SHA1_LEN] {
                &self.0
            }

            /// The 40-character lowercase hexadecimal form.
            pub fn to_hex(&self) -> String {
                self.to_string()
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write_hex(f, &self.0)
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "("))?;
                write_hex(f, &self.0)?;
                write!(f, ")")
            }
        }

        impl FromStr for $name {
            type Err = IdError;
            fn from_str(s: &str) -> Result<Self, IdError> {
                Self::parse_hex(s)
            }
        }
    };
}

sha1_newtype!(
    /// The SHA-1 ID of a Git object (commit, tree, blob or tag).
    ///
    /// The all-zero ID is Git's "null" ID. On the wire it means "no such ref"
    /// in a ref update's old or new value; [`ObjectId::is_zero`] tests for it.
    /// SHA-256 repositories are out of scope for v1 (`docs/PLAN.md`, section 1).
    ObjectId
);

impl ObjectId {
    /// Git's null ID, forty zeros.
    pub const ZERO: ObjectId = ObjectId([0; SHA1_LEN]);

    /// Whether this is Git's null ID.
    pub fn is_zero(&self) -> bool {
        self.0 == [0; SHA1_LEN]
    }
}

sha1_newtype!(
    /// The trailing SHA-1 of a stored pack file: the checksum over every byte
    /// before it.
    ///
    /// It identifies the exact stored bytes. It is the hash advertised in a
    /// `packfile-uris` line, and the pack's blob keys are derived from it
    /// (see [`crate::pack::PackKeys`]), so one key can never serve two
    /// different byte sequences.
    PackChecksum
);

macro_rules! row_id {
    ($(#[$doc:meta])* $name:ident) => {
        $(#[$doc])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(i64);

        impl $name {
            /// Wraps a database row ID. Row IDs come from an identity
            /// column that starts at 1, so zero and negative values are
            /// rejected.
            pub const fn new(raw: i64) -> Result<Self, IdError> {
                if raw > 0 { Ok(Self(raw)) } else { Err(IdError::NotPositive(raw)) }
            }

            /// The raw database value.
            pub const fn get(self) -> i64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }
    };
}

row_id!(
    /// Row ID of a repository (`repos.id`).
    RepoId
);
row_id!(
    /// Row ID of one version of one ref (`ref_versions.id`).
    ///
    /// IDs are assigned in insertion order, so a version's dependencies and
    /// parent always have smaller IDs than the version itself; schema v1
    /// enforces this with `CHECK` constraints, which makes the dependency
    /// graph acyclic.
    RefVersionId
);
row_id!(
    /// Row ID of a stored pack (`packs.id`).
    PackId
);
row_id!(
    /// Row ID of a compaction cover (`covers.id`).
    CoverId
);
row_id!(
    /// Row ID of a background job (`jobs.id`).
    JobId
);

/// A point in time, as microseconds since the Unix epoch (UTC).
///
/// Schema v1 stores every timestamp in this form (`*_us BIGINT` columns), in
/// Postgres and SQLite alike, so the two dialects stay identical.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct UnixMicros(pub i64);

impl UnixMicros {
    /// The current time from the system clock.
    pub fn now() -> Self {
        Self::from_system_time(std::time::SystemTime::now())
    }

    /// Converts a [`std::time::SystemTime`], saturating outside the `i64`
    /// range.
    pub fn from_system_time(t: std::time::SystemTime) -> Self {
        match t.duration_since(std::time::UNIX_EPOCH) {
            Ok(d) => Self(i64::try_from(d.as_micros()).unwrap_or(i64::MAX)),
            Err(e) => Self(
                i64::try_from(e.duration().as_micros())
                    .map(|v| -v)
                    .unwrap_or(i64::MIN),
            ),
        }
    }

    /// This time plus a duration, saturating.
    pub fn saturating_add(self, d: std::time::Duration) -> Self {
        Self(
            self.0
                .saturating_add(i64::try_from(d.as_micros()).unwrap_or(i64::MAX)),
        )
    }
}

/// The largest value a `BIGINT` column holds. Every count, byte size and
/// sequence number Aspen stores is at most this; Rust-side validation
/// checks it so a value never fails only when it reaches the database.
pub const DB_MAX: u64 = i64::MAX as u64;

/// A ref version's position in its ref's history (`ref_versions.seq`,
/// `refs.seq`, `covers.first_seq`/`last_seq`): 1 for the first version,
/// at most [`DB_MAX`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(u64);

impl Seq {
    /// The first version of a ref.
    pub const FIRST: Seq = Seq(1);

    /// A sequence number in `1..=DB_MAX`.
    pub const fn new(raw: u64) -> Result<Self, IdError> {
        if raw >= 1 && raw <= DB_MAX {
            Ok(Self(raw))
        } else {
            Err(IdError::SeqOutOfRange(raw as i128))
        }
    }

    /// A sequence number read from a database column.
    pub const fn from_db(raw: i64) -> Result<Self, IdError> {
        if raw >= 1 {
            Ok(Self(raw as u64))
        } else {
            Err(IdError::SeqOutOfRange(raw as i128))
        }
    }

    /// The number.
    pub const fn get(self) -> u64 {
        self.0
    }

    /// The number as stored in a `BIGINT` column; never negative.
    pub const fn to_db(self) -> i64 {
        self.0 as i64
    }

    /// The next version's number; `None` at [`DB_MAX`].
    pub const fn next(self) -> Option<Seq> {
        if self.0 < DB_MAX {
            Some(Self(self.0 + 1))
        } else {
            None
        }
    }

    /// The number `back` versions earlier; `None` if that is before 1.
    pub const fn back(self, back: u64) -> Option<Seq> {
        match self.0.checked_sub(back) {
            Some(s) if s >= 1 => Some(Self(s)),
            _ => None,
        }
    }
}

impl fmt::Display for Seq {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const HEX: &str = "0123456789abcdef0123456789abcdef01234567";

    #[test]
    fn object_id_round_trips_through_hex() {
        let id = ObjectId::parse_hex(HEX).unwrap();
        assert_eq!(id.to_hex(), HEX);
        assert_eq!(id.to_string(), HEX);
        assert_eq!(format!("{id:?}"), format!("ObjectId({HEX})"));
        assert_eq!(HEX.parse::<ObjectId>().unwrap(), id);
        assert_eq!(ObjectId::from_slice(id.as_bytes()).unwrap(), id);
    }

    #[test]
    fn object_id_rejects_uppercase_and_non_hex() {
        let upper = HEX.replace('a', "A");
        assert_eq!(
            ObjectId::parse_hex(&upper),
            Err(IdError::NotLowercaseHex { position: 10 })
        );
        let mut bad = HEX.to_owned();
        bad.replace_range(39..40, "g");
        assert_eq!(
            ObjectId::parse_hex(&bad),
            Err(IdError::NotLowercaseHex { position: 39 })
        );
        // Multi-byte UTF-8 is not a hex digit and must not panic.
        let mut utf8 = HEX[..38].to_owned();
        utf8.push('é');
        assert_eq!(
            ObjectId::parse_hex(&utf8),
            Err(IdError::NotLowercaseHex { position: 38 })
        );
    }

    #[test]
    fn object_id_rejects_wrong_lengths() {
        assert_eq!(
            ObjectId::parse_hex(&HEX[..39]),
            Err(IdError::WrongLength {
                expected: 40,
                found: 39
            })
        );
        assert_eq!(
            ObjectId::from_slice(&[0; 19]),
            Err(IdError::WrongLength {
                expected: 20,
                found: 19
            })
        );
    }

    #[test]
    fn zero_id_is_git_null() {
        assert!(ObjectId::ZERO.is_zero());
        assert_eq!(ObjectId::ZERO.to_hex(), "0".repeat(40));
        assert!(!ObjectId::parse_hex(HEX).unwrap().is_zero());
    }

    #[test]
    fn object_ids_order_bytewise_like_an_idx() {
        let a = ObjectId::parse_hex(&"0".repeat(40)).unwrap();
        let b = ObjectId::parse_hex(&format!("{}1", "0".repeat(39))).unwrap();
        let c = ObjectId::parse_hex(&format!("1{}", "0".repeat(39))).unwrap();
        assert!(a < b && b < c);
    }

    #[test]
    fn checksum_is_a_distinct_type_with_its_own_debug() {
        let c = PackChecksum::parse_hex(HEX).unwrap();
        assert_eq!(format!("{c:?}"), format!("PackChecksum({HEX})"));
    }

    #[test]
    fn row_ids_must_be_positive() {
        assert_eq!(PackId::new(7).unwrap().get(), 7);
        assert_eq!(RepoId::new(0), Err(IdError::NotPositive(0)));
        assert_eq!(RefVersionId::new(-3), Err(IdError::NotPositive(-3)));
    }

    #[test]
    fn seqs_stay_in_the_bigint_range() {
        assert_eq!(Seq::new(1), Ok(Seq::FIRST));
        assert_eq!(Seq::new(0), Err(IdError::SeqOutOfRange(0)));
        assert_eq!(
            Seq::new(DB_MAX + 1),
            Err(IdError::SeqOutOfRange(i128::from(DB_MAX) + 1))
        );
        assert_eq!(Seq::from_db(-1), Err(IdError::SeqOutOfRange(-1)));
        assert_eq!(Seq::from_db(i64::MAX).unwrap().get(), DB_MAX);
        assert_eq!(Seq::new(DB_MAX).unwrap().next(), None);
        assert_eq!(Seq::FIRST.next(), Some(Seq::new(2).unwrap()));
        assert_eq!(Seq::new(5).unwrap().back(4), Some(Seq::FIRST));
        assert_eq!(Seq::new(5).unwrap().back(5), None);
        assert_eq!(Seq::new(7).unwrap().to_db(), 7);
    }

    #[test]
    fn unix_micros_converts_both_sides_of_the_epoch() {
        use std::time::{Duration, UNIX_EPOCH};
        assert_eq!(
            UnixMicros::from_system_time(UNIX_EPOCH + Duration::from_micros(5)),
            UnixMicros(5)
        );
        assert_eq!(
            UnixMicros::from_system_time(UNIX_EPOCH - Duration::from_micros(5)),
            UnixMicros(-5)
        );
        assert_eq!(
            UnixMicros(1).saturating_add(Duration::from_secs(1)),
            UnixMicros(1_000_001)
        );
    }
}
