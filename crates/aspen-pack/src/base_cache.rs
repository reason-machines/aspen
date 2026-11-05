//! A delta-base cache with a byte budget, for reading many objects from
//! the same stored packs one at a time (gix's `Bundle::find`).
//!
//! Resolving a delta decodes its whole chain. Without a cache, reading
//! every tree of a history, or undeltifying many entries of one pack,
//! decodes the shared bases of those chains again for each object, which
//! costs O(objects × depth). gix offers LRU caches only behind optional
//! features with extra dependencies, so this is a plain one: decoded
//! objects keyed by `(source, offset)` (gix stores each delta it
//! resolves, and checks the cache at every delta of a chain it walks), evicted oldest first once their
//! total size passes the budget, like Git's `core.deltaBaseCacheLimit`.

use std::collections::{HashMap, VecDeque};

/// Git's default `core.deltaBaseCacheLimit`: 96 MiB.
pub(crate) const DEFAULT_BASE_CACHE_BYTES: usize = 96 << 20;

/// Decoded delta bases of several packs, at most `budget` bytes in all.
pub(crate) struct BaseCache {
    budget: usize,
    held: usize,
    /// Decoded object, its kind and the size of its entry's compressed
    /// data, which gix asks back on a hit.
    objects: HashMap<(u32, u64), Cached>,
    /// Insertion order, for eviction.
    order: VecDeque<(u32, u64)>,
}

impl BaseCache {
    /// An empty cache holding at most `budget` bytes of object data.
    pub(crate) fn new(budget: usize) -> Self {
        Self {
            budget,
            held: 0,
            objects: HashMap::new(),
            order: VecDeque::new(),
        }
    }

    /// The cache as gix sees it for one source. gix's own pack IDs come from
    /// a hash of the path, so the caller's source index is used instead.
    pub(crate) fn for_source(&mut self, source: u32) -> SourceCache<'_> {
        SourceCache {
            cache: self,
            source,
        }
    }
}

struct Cached {
    kind: gix_object::Kind,
    data: Vec<u8>,
    compressed_size: usize,
}

/// [`BaseCache`] for the pack of one source.
pub(crate) struct SourceCache<'a> {
    cache: &'a mut BaseCache,
    source: u32,
}

impl gix_pack::cache::DecodeEntry for SourceCache<'_> {
    fn put(
        &mut self,
        _pack_id: u32,
        offset: u64,
        data: &[u8],
        kind: gix_object::Kind,
        compressed_size: usize,
    ) {
        let c = &mut *self.cache;
        let key = (self.source, offset);
        if data.len() > c.budget || c.objects.contains_key(&key) {
            return;
        }
        while c.held + data.len() > c.budget {
            let Some(old) = c.order.pop_front() else {
                break;
            };
            if let Some(old) = c.objects.remove(&old) {
                c.held -= old.data.len();
            }
        }
        c.held += data.len();
        c.objects.insert(
            key,
            Cached {
                kind,
                data: data.to_vec(),
                compressed_size,
            },
        );
        c.order.push_back(key);
    }

    fn get(
        &mut self,
        _pack_id: u32,
        offset: u64,
        out: &mut Vec<u8>,
    ) -> Option<(gix_object::Kind, usize)> {
        let hit = self.cache.objects.get(&(self.source, offset))?;
        out.clear();
        out.extend_from_slice(&hit.data);
        Some((hit.kind, hit.compressed_size))
    }
}
