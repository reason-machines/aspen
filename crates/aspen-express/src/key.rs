//! The only blobs the express serves: stored packs, named by their
//! canonical key `repos/<repo id>/packs/<checksum>.pack`, or
//! `repos/<repo id>/packs/<checksum>.g<n>.pack` for a checksum registered
//! again after garbage collection ([`aspen_core::pack::PackKeys`]).
//!
//! The checksum in the key is the SHA-1 trailer of the pack's bytes and
//! blobs are write-once (A4), so the bytes behind a key never change. That
//! is what makes an express URL immutable and its response safe for any
//! cache to share.

use aspen_core::blob::BlobKey;
use aspen_core::id::{PackChecksum, RepoId};
use aspen_core::pack::{PackGeneration, PackKeys, PackRecord};

/// A canonical pack key and the checksum it names.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PackKey {
    key: BlobKey,
    checksum: PackChecksum,
}

impl PackKey {
    /// The pack key of `repo`'s pack with this checksum and generation.
    pub fn new(repo: RepoId, checksum: PackChecksum, generation: PackGeneration) -> Self {
        Self {
            key: PackKeys::new(repo, &checksum, generation).pack,
            checksum,
        }
    }

    /// The pack key of a stored pack.
    pub fn of(pack: &PackRecord) -> Self {
        Self {
            key: pack.keys().pack,
            checksum: pack.pack.checksum,
        }
    }

    /// Parses a canonical pack key. Anything else (an `.idx`, an entry
    /// table, another spelling of the repository ID or checksum) is `None`,
    /// so one pack has exactly one key and one URL per expiry.
    pub fn parse(s: &str) -> Option<Self> {
        let rest = s.strip_prefix("repos/")?;
        let (repo, file) = rest.split_once("/packs/")?;
        let name = file.strip_suffix(".pack")?;
        let (hex, generation) = match name.split_once(".g") {
            Some((hex, n)) => (hex, PackGeneration::new(n.parse().ok()?)),
            None => (name, PackGeneration::FIRST),
        };
        let repo = RepoId::new(repo.parse().ok()?).ok()?;
        let checksum = PackChecksum::parse_hex(hex).ok()?;
        // Re-derived and compared, so every other spelling (`.g0`, a
        // leading zero, a sign) is refused.
        let key = Self::new(repo, checksum, generation);
        (key.key.as_str() == s).then_some(key)
    }

    /// Accepts `key` if it is a canonical pack key.
    pub fn from_blob_key(key: &BlobKey) -> Option<Self> {
        Self::parse(key.as_str())
    }

    /// The blob key.
    pub fn blob_key(&self) -> &BlobKey {
        &self.key
    }

    /// The pack's checksum (its SHA-1 trailer).
    pub fn checksum(&self) -> &PackChecksum {
        &self.checksum
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The express serves a stored pack under the key its row names,
    /// generation included, so a checksum registered again after garbage
    /// collection is read from its new blobs.
    #[test]
    fn a_stored_packs_key_carries_its_generation() {
        let record = PackRecord {
            id: aspen_core::id::PackId::new(9).unwrap(),
            pack: aspen_core::pack::NewPack {
                repo: RepoId::new(7).unwrap(),
                checksum: PackChecksum::from_bytes([0xab; 20]),
                size: 100,
                idx_size: 1072 + 28,
                entries_size: aspen_core::pack::entry_table_len(1),
                object_count: 1,
                ref_delta_count: 0,
                origin: aspen_core::pack::PackOrigin::Push { received_bytes: 80 },
            },
            generation: PackGeneration::new(2),
            created_at: aspen_core::id::UnixMicros(0),
        };
        let key = PackKey::of(&record);
        assert_eq!(key.blob_key(), &record.keys().pack);
        assert_eq!(PackKey::parse(key.blob_key().as_str()), Some(key));
    }

    #[test]
    fn only_canonical_pack_keys_parse() {
        let hex = "0123456789abcdef0123456789abcdef01234567";
        let ok = format!("repos/7/packs/{hex}.pack");
        let key = PackKey::parse(&ok).unwrap();
        assert_eq!(key.blob_key().as_str(), ok);
        assert_eq!(key.checksum().to_hex(), hex);
        let g3 = format!("repos/7/packs/{hex}.g3.pack");
        let key = PackKey::parse(&g3).unwrap();
        assert_eq!(key.blob_key().as_str(), g3);
        assert_eq!(key.checksum().to_hex(), hex);
        for bad in [
            format!("repos/7/packs/{hex}.idx"),
            format!("repos/7/packs/{hex}.g0.pack"),
            format!("repos/7/packs/{hex}.g03.pack"),
            format!("repos/7/packs/{hex}.g+3.pack"),
            format!("repos/7/packs/{hex}.g.pack"),
            format!("repos/7/packs/{hex}.g3.idx"),
            format!("repos/7/packs/{hex}.g4294967296.pack"),
            format!("repos/7/packs/{hex}.entries"),
            format!("repos/07/packs/{hex}.pack"),
            format!("repos/0/packs/{hex}.pack"),
            format!("repos/-1/packs/{hex}.pack"),
            format!("repos/7/packs/{}.pack", hex.to_uppercase()),
            format!("repos/7/packs/{}.pack", &hex[1..]),
            format!("repos/7/x/packs/{hex}.pack"),
            format!("/repos/7/packs/{hex}.pack"),
            format!("repos/7/packs/{hex}.pack/"),
            "repos/7/packs/.pack".to_owned(),
            String::new(),
        ] {
            assert_eq!(PackKey::parse(&bad), None, "{bad}");
        }
    }
}
