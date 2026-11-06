//! The chain map of a pack whose `.idx` is sampled ([`crate::sampled`]),
//! kept in a file on local disk: for every entry, in pack order, its offset
//! and its delta base's offset. With it, the locator knows every entry of a
//! delta chain before it reads any of them, and reads them all in one round
//! of parallel range reads instead of one round per level (monorepo
//! benchmark, item 11: chains of depth 50 cost about 1 s on S3).
//!
//! Packs with a whole `.idx` keep their chain map in memory
//! ([`crate::locate`]); a sampled pack is one too large for that (over 2.4
//! million objects by default), so its map is a file of 16 bytes per entry,
//! deleted when the map is dropped, with one offset of every [`BLOCK`]
//! entries in memory. Finding an entry reads one block of the file (4 KiB,
//! normally from the page cache).
//!
//! The map is built from the pack's entry table, streamed once when the
//! sample is built, and checked as it streams: it must describe the pack
//! its row names, list entries in increasing offsets inside the pack, and
//! give every OFS_DELTA an earlier base. A REF_DELTA's base is named by ID,
//! which a sampled `.idx` cannot place without reading, so the map leaves it
//! out and the locator finds it by reading, as without a map. The map is a
//! shortcut: it decides only which entries are read up front, never how
//! one is resolved (each entry read names its own base), and a chain it
//! cannot follow is read level by level.

use std::fs::File;
use std::io::{BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::sync::Mutex;

use aspen_core::pack::{
    ENTRY_RECORD_LEN, ENTRY_TABLE_HEADER_LEN, ENTRY_TABLE_MAGIC, ENTRY_TABLE_VERSION,
    PACK_HEADER_LEN, PACK_TRAILER_LEN, PackRecord,
};

/// Entries per block of the file, and per offset kept in memory.
pub const BLOCK: u64 = 256;

/// Bytes per entry in the file: offset and base offset, big-endian.
const RECORD: usize = 16;

/// The base of a full object.
const FULL: u64 = u64::MAX;
/// The base of a REF_DELTA (named by ID, not placed).
const UNPLACED: u64 = u64::MAX - 1;

/// A sampled pack's chain map (module docs).
#[derive(Debug)]
pub struct DiskChains {
    file: Mutex<File>,
    count: u64,
    /// The offset of the first entry of every block.
    firsts: Vec<u64>,
}

impl DiskChains {
    /// Bytes held in memory.
    pub fn cost(&self) -> u64 {
        (self.firsts.len() * std::mem::size_of::<u64>()) as u64
    }

    /// The entry at `offset` and the entries below it on its delta chain,
    /// in that order, appended to `out` as `(pack, offset)`. Stops (leaving
    /// the rest to be found by reading) at an offset the map does not list,
    /// a REF_DELTA, or a local read error.
    pub fn chain(&self, pack: usize, offset: u64, out: &mut Vec<(usize, u64)>) {
        out.push((pack, offset));
        let mut file = self.file.lock().expect("not poisoned");
        let mut held: Option<(usize, Vec<u8>)> = None;
        let mut at = offset;
        for _ in 0..self.count {
            let b = self.firsts.partition_point(|&f| f <= at);
            if b == 0 {
                return;
            }
            let b = b - 1;
            if held.as_ref().is_none_or(|(h, _)| *h != b) {
                match self.read_block(&mut file, b) {
                    Ok(bytes) => held = Some((b, bytes)),
                    Err(_) => return,
                }
            }
            let block = &held.as_ref().expect("read").1;
            let records = block.as_chunks::<RECORD>().0;
            let Ok(i) = records.binary_search_by_key(&at, |r| u64_at(r, 0)) else {
                return;
            };
            match u64_at(&records[i], 8) {
                FULL | UNPLACED => return,
                base => {
                    out.push((pack, base));
                    at = base;
                }
            }
        }
    }

    fn read_block(&self, file: &mut File, b: usize) -> std::io::Result<Vec<u8>> {
        let first = b as u64 * BLOCK;
        let n = BLOCK.min(self.count - first) as usize;
        let mut bytes = vec![0; n * RECORD];
        file.seek(SeekFrom::Start(first * RECORD as u64))?;
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

fn u64_at(b: &[u8], at: usize) -> u64 {
    u64::from_be_bytes(b[at..at + 8].try_into().expect("8 bytes"))
}

/// Why a chain map could not be built.
#[derive(Debug, thiserror::Error)]
pub enum ChainsError {
    /// The entry table does not describe the pack, or is malformed.
    #[error("entry table: {0}")]
    Corrupt(String),
    /// Writing the local file failed.
    #[error("chain map file: {0}")]
    Io(#[from] std::io::Error),
}

/// Builds a [`DiskChains`] from a pack's entry table, fed in order.
#[derive(Debug)]
pub struct ChainsBuilder {
    file: BufWriter<File>,
    count: u64,
    seen: u64,
    last: Option<u64>,
    data_end: u64,
    firsts: Vec<u64>,
}

impl ChainsBuilder {
    /// The range of the entry table's records `[from, to)`, by record index.
    pub fn records_range(from: u64, to: u64) -> (u64, u64) {
        let at = |i: u64| ENTRY_TABLE_HEADER_LEN as u64 + ENTRY_RECORD_LEN as u64 * i;
        (at(from), at(to))
    }

    /// The range of the header.
    pub fn header_range() -> (u64, u64) {
        (0, ENTRY_TABLE_HEADER_LEN as u64)
    }

    /// Starts a map in a new file in `dir` (deleted when the map is
    /// dropped), checking the table's `header` against the pack's `row`.
    pub fn new(dir: &Path, row: &PackRecord, header: &[u8]) -> Result<Self, ChainsError> {
        let count = u64::from(row.pack.object_count);
        if header.len() != ENTRY_TABLE_HEADER_LEN
            || header[..8] != ENTRY_TABLE_MAGIC
            || header[8..12] != ENTRY_TABLE_VERSION.to_be_bytes()
            || u64::from(u32::from_be_bytes(header[12..16].try_into().expect("4"))) != count
            || header[16..24] != row.pack.size.to_be_bytes()
            || header[24..44] != *row.pack.checksum.as_bytes()
            || row.pack.entries_size
                != ENTRY_TABLE_HEADER_LEN as u64 + ENTRY_RECORD_LEN as u64 * count
        {
            return Err(ChainsError::Corrupt("does not describe this pack".into()));
        }
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            file: BufWriter::with_capacity(1 << 16, tempfile::tempfile_in(dir)?),
            count,
            seen: 0,
            last: None,
            data_end: row.pack.size.saturating_sub(PACK_TRAILER_LEN),
            firsts: Vec::with_capacity(count.div_ceil(BLOCK) as usize),
        })
    }

    /// The entry count, to plan the reads of its records.
    pub fn count(&self) -> u64 {
        self.count
    }

    /// Feeds the next records, whole, in order.
    pub fn feed(&mut self, records: &[u8]) -> Result<(), ChainsError> {
        let bad = |what: String| Err(ChainsError::Corrupt(what));
        if !records.len().is_multiple_of(ENTRY_RECORD_LEN) {
            return bad("a read ends inside a record".into());
        }
        for rec in records.as_chunks::<ENTRY_RECORD_LEN>().0 {
            if self.seen == self.count {
                return bad("more records than entries".into());
            }
            let offset = u64_at(rec, 0);
            let ordered = match self.last {
                None => offset == PACK_HEADER_LEN,
                Some(l) => offset > l,
            };
            if !ordered || offset >= self.data_end {
                return bad(format!("an entry at {offset} is out of order or place"));
            }
            let base = match rec[8] {
                1..=4 => FULL,
                6 => {
                    let b = u64_at(rec, 32);
                    if b >= offset || b < PACK_HEADER_LEN {
                        return bad(format!("the OFS_DELTA at {offset} has no earlier base"));
                    }
                    b
                }
                7 => UNPLACED,
                t => return bad(format!("unknown entry type {t} at {offset}")),
            };
            if self.seen.is_multiple_of(BLOCK) {
                self.firsts.push(offset);
            }
            self.file.write_all(&offset.to_be_bytes())?;
            self.file.write_all(&base.to_be_bytes())?;
            self.last = Some(offset);
            self.seen += 1;
        }
        Ok(())
    }

    /// The map, once every record was fed.
    pub fn finish(self) -> Result<DiskChains, ChainsError> {
        if self.seen != self.count {
            return Err(ChainsError::Corrupt(format!(
                "{} records for {} entries",
                self.seen, self.count
            )));
        }
        let file = self.file.into_inner().map_err(|e| e.into_error())?;
        Ok(DiskChains {
            file: Mutex::new(file),
            count: self.count,
            firsts: self.firsts,
        })
    }
}
