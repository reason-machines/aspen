//! A cursor that turns fuzzer bytes into decisions.
//!
//! Every read succeeds: once the bytes run out it returns zeros, so any
//! input is a valid program for a structured target and libFuzzer's
//! mutations (which flip, insert and drop bytes) always reach the code
//! under test. No `arbitrary` crate: the format is a few lines and keeps
//! the dependency graph unchanged.

/// Reads decisions from fuzzer bytes.
#[derive(Debug, Clone)]
pub struct Input<'a> {
    data: &'a [u8],
}

impl<'a> Input<'a> {
    /// A cursor over `data`.
    pub fn new(data: &'a [u8]) -> Self {
        Self { data }
    }

    /// Whether every byte has been read.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// The next byte, or 0.
    pub fn byte(&mut self) -> u8 {
        match self.data.split_first() {
            Some((b, rest)) => {
                self.data = rest;
                *b
            }
            None => 0,
        }
    }

    /// A flag.
    pub fn flag(&mut self) -> bool {
        self.byte() & 1 == 1
    }

    /// A flag that is set about once in `n` reads, and always when `n` is
    /// 0 or 1. Never set once the bytes run out, so a short input takes
    /// the common path rather than every rare one.
    pub fn one_in(&mut self, n: u8) -> bool {
        n <= 1 || self.byte() % n == 1
    }

    /// A little-endian `u16`.
    pub fn u16(&mut self) -> u16 {
        u16::from_le_bytes([self.byte(), self.byte()])
    }

    /// A little-endian `u32`.
    pub fn u32(&mut self) -> u32 {
        u32::from_le_bytes([self.byte(), self.byte(), self.byte(), self.byte()])
    }

    /// A little-endian `u64`.
    pub fn u64(&mut self) -> u64 {
        u64::from(self.u32()) | (u64::from(self.u32()) << 32)
    }

    /// A number in `0..n`; 0 when `n` is 0.
    pub fn below(&mut self, n: usize) -> usize {
        match n {
            0 | 1 => 0,
            n if n <= 256 => usize::from(self.byte()) % n,
            n if n <= 65_536 => usize::from(self.u16()) % n,
            n => (self.u32() as usize) % n,
        }
    }

    /// Up to `max` bytes (a length byte, then the bytes).
    pub fn bytes(&mut self, max: usize) -> &'a [u8] {
        let want = self.below(max.saturating_add(1));
        let n = want.min(self.data.len());
        let (head, rest) = self.data.split_at(n);
        self.data = rest;
        head
    }

    /// Every byte not read yet.
    pub fn rest(self) -> &'a [u8] {
        self.data
    }
}

/// SplitMix64, for decisions that must not consume the bytes a target
/// passes on whole (the wire stream of a protocol target).
#[derive(Debug, Clone)]
pub struct Mix(u64);

impl Mix {
    /// A generator seeded with `seed`.
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next value.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A number in `0..n`; 0 when `n` is 0.
    pub fn below(&mut self, n: usize) -> usize {
        if n == 0 {
            0
        } else {
            (self.next_u64() % n as u64) as usize
        }
    }

    /// A flag.
    pub fn flag(&mut self) -> bool {
        self.next_u64() & 1 == 1
    }
}

/// Splits a protocol target's input into its 9-byte header (one
/// configuration byte and an 8-byte seed for [`Mix`]) and the wire bytes.
/// Short inputs get zeros, so every input is valid.
pub fn split_header(data: &[u8]) -> (u8, Mix, &[u8]) {
    let mut head = [0u8; 9];
    let n = data.len().min(9);
    head[..n].copy_from_slice(&data[..n]);
    let seed = u64::from_le_bytes(head[1..9].try_into().expect("8 bytes"));
    (head[0], Mix::new(seed), &data[n..])
}

/// The header [`split_header`] reads, for seed files.
pub fn header(config: u8, seed: u64) -> Vec<u8> {
    let mut v = vec![config];
    v.extend_from_slice(&seed.to_le_bytes());
    v
}
