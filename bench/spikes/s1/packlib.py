"""Minimal Git pack v2 reader and the two stitchers tested by spike S1.

A pack is: "PACK", version (u32 BE), object count (u32 BE), entries, SHA-1 of
everything before the trailer. Each entry is a type/size varint header, an
optional delta base (OFS_DELTA: negative offset varint; REF_DELTA: 20-byte
object id), then a zlib stream.

stitch_plain() is the algorithm in docs/PLAN.md: concatenate entries, rewrite
the count, recompute the trailer. Nothing is inflated or recompressed.

stitch_topo() is the recommended fix for what the spike found: it drops
duplicate object ids and emits every delta after its base, rewriting only entry
headers (OFS_DELTA distances). It needs each segment's offset -> object id
table, which a server already has from the pack's index.

stitch_naive_dedupe() is kept as a negative result: it can create delta cycles.
"""

from __future__ import annotations

import hashlib
import struct
import zlib
from dataclasses import dataclass

OBJ_COMMIT, OBJ_TREE, OBJ_BLOB, OBJ_TAG, OBJ_OFS_DELTA, OBJ_REF_DELTA = 1, 2, 3, 4, 6, 7
TYPE_NAMES = {1: "commit", 2: "tree", 3: "blob", 4: "tag", 6: "ofs_delta", 7: "ref_delta"}


@dataclass
class Entry:
    offset: int  # offset of the entry header within its own pack
    end: int  # offset one past the end of its zlib stream
    type: int
    size: int  # inflated size from the header (delta size for deltas)
    header_len: int  # bytes of type/size header
    base_offset: int | None = None  # OFS_DELTA: absolute offset of the base in this pack
    base_oid: bytes | None = None  # REF_DELTA: base object id
    data_start: int = 0  # offset where the zlib stream starts


class PackError(Exception):
    pass


def parse_pack(buf: bytes, verify: bool = True) -> tuple[int, list[Entry]]:
    """Return (declared object count, entries). Raises PackError on malformed input."""
    if len(buf) < 32 or buf[:4] != b"PACK":
        raise PackError("not a pack")
    version, count = struct.unpack(">II", buf[4:12])
    if version not in (2, 3):
        raise PackError(f"unsupported pack version {version}")
    if verify and hashlib.sha1(buf[:-20]).digest() != buf[-20:]:
        raise PackError("trailer checksum mismatch")
    end_of_entries = len(buf) - 20
    pos = 12
    entries: list[Entry] = []
    for _ in range(count):
        start = pos
        c = buf[pos]
        pos += 1
        typ = (c >> 4) & 7
        size = c & 15
        shift = 4
        while c & 0x80:
            c = buf[pos]
            pos += 1
            size |= (c & 0x7F) << shift
            shift += 7
        header_len = pos - start
        e = Entry(offset=start, end=0, type=typ, size=size, header_len=header_len)
        if typ == OBJ_OFS_DELTA:
            c = buf[pos]
            pos += 1
            ofs = c & 0x7F
            while c & 0x80:
                c = buf[pos]
                pos += 1
                ofs = ((ofs + 1) << 7) | (c & 0x7F)
            e.base_offset = start - ofs
        elif typ == OBJ_REF_DELTA:
            e.base_oid = buf[pos : pos + 20]
            pos += 20
        elif typ not in (OBJ_COMMIT, OBJ_TREE, OBJ_BLOB, OBJ_TAG):
            raise PackError(f"bad object type {typ} at {start}")
        e.data_start = pos
        d = zlib.decompressobj()
        out = d.decompress(buf[pos:end_of_entries])
        if not d.eof:
            raise PackError(f"truncated zlib stream at {start}")
        if len(out) != size:
            raise PackError(f"size mismatch at {start}: header {size}, inflated {len(out)}")
        pos = end_of_entries - len(d.unused_data)
        e.end = pos
        entries.append(e)
    if pos != end_of_entries:
        raise PackError(f"{end_of_entries - pos} trailing bytes after {count} entries")
    return count, entries


def encode_header(typ: int, size: int) -> bytes:
    out = bytearray()
    c = (typ << 4) | (size & 15)
    size >>= 4
    while size:
        out.append(c | 0x80)
        c = size & 0x7F
        size >>= 7
    out.append(c)
    return bytes(out)


def _finish(count: int, body: bytes | bytearray) -> bytes:
    head = b"PACK" + struct.pack(">II", 2, count)
    h = hashlib.sha1(head)
    h.update(body)
    return head + bytes(body) + h.digest()


def stitch_plain(segments: list[bytes]) -> bytes:
    """Concatenate entries, rewrite the count, recompute the trailer."""
    body = bytearray()
    total = 0
    for seg in segments:
        if seg[:4] != b"PACK" or struct.unpack(">I", seg[4:8])[0] != 2:
            raise PackError("segment is not a v2 pack")
        total += struct.unpack(">I", seg[8:12])[0]
        body += seg[12:-20]
    return _finish(total, body)


def encode_ofs(distance: int) -> bytes:
    """Git's OFS_DELTA base-distance varint (big-endian, with the +1 bias)."""
    out = [distance & 0x7F]
    distance >>= 7
    while distance:
        distance -= 1
        out.append(0x80 | (distance & 0x7F))
        distance >>= 7
    return bytes(reversed(out))


def full_entry(typ: int, data: bytes) -> bytes:
    """A non-delta entry built from an object's content (compression happens here)."""
    return encode_header(typ, len(data)) + zlib.compress(data)


def stitch_naive_dedupe(segments: list[bytes], oid_maps: list[dict[int, bytes]]) -> tuple[bytes, int]:
    """Keep the first copy of each object id; rewrite OFS_DELTA to REF_DELTA in segments that lost entries.

    UNSAFE, kept as a documented negative result: with thin segments it can
    produce a delta cycle (X's kept copy is a delta on Y, Y's kept copy is a
    delta on X). See README.md, finding "naive dedupe makes delta cycles".
    """
    seen: set[bytes] = set()
    body = bytearray()
    total = dropped = 0
    for seg, oids in zip(segments, oid_maps):
        _, entries = parse_pack(seg)
        keep = []
        for e in entries:
            oid = oids[e.offset]
            if oid in seen:
                continue
            seen.add(oid)
            keep.append(e)
        dropped += len(entries) - len(keep)
        total += len(keep)
        if len(keep) == len(entries):
            body += seg[12:-20]
            continue
        for e in keep:
            if e.type == OBJ_OFS_DELTA:
                body += encode_header(OBJ_REF_DELTA, e.size)
                body += oids[e.base_offset]
                body += seg[e.data_start : e.end]
            else:
                body += seg[e.offset : e.end]
    return _finish(total, body), dropped


class Unstitchable(Exception):
    """Some objects can only be sent as deltas whose bases can never be emitted first."""


def stitch_topo(
    segments: list[bytes],
    oid_maps: list[dict[int, bytes]],
    allow_thin: bool,
    prefix: list[tuple[bytes, int, bytes]] = (),
) -> tuple[bytes, dict]:
    """Stitch with deduplication and base-before-delta order. Never recompresses segment data.

    Rules, applied in passes over the segments in order:
      * the first copy of an object id to become emittable wins; later copies are dropped;
      * a non-delta entry is always emittable;
      * a delta entry is emittable once its base object id has been emitted, or,
        if allow_thin, when its base is in no segment at all (an external base
        the client is expected to have);
      * a base that is anywhere in the stitched object set must be emitted
        before any delta that uses it. This is what rules out delta cycles.

    OFS_DELTA entries get their base distance recomputed against wherever the
    base was emitted (any copy of the base object will do). REF_DELTA entries
    are copied as they are. Entry data (the zlib streams) is copied byte for
    byte; only headers change. If nothing is duplicated or reordered the output
    is identical to stitch_plain().

    prefix is a list of (oid, type, content) full objects emitted first: the
    fallback for clients that did not ask for thin-pack (their external bases
    are sent whole, which does need compression).
    Raises Unstitchable if some objects can never be emitted.
    """
    items = []  # (seg index, Entry, oid, base oid or None)
    for i, (seg, oids) in enumerate(zip(segments, oid_maps)):
        _, entries = parse_pack(seg)
        for e in entries:
            base = None
            if e.type == OBJ_OFS_DELTA:
                base = oids[e.base_offset]
            elif e.type == OBJ_REF_DELTA:
                base = e.base_oid
            items.append((i, e, oids[e.offset], base))
    in_set = {it[2] for it in items} | {p[0] for p in prefix}

    body = bytearray()
    where: dict[bytes, int] = {}  # oid -> offset of its emitted entry in the output pack
    for oid, typ, data in prefix:
        if oid not in where:
            where[oid] = 12 + len(body)
            body += full_entry(typ, data)
    pending = list(range(len(items)))
    passes = 0
    moved = 0
    while pending:
        passes += 1
        still = []
        progress = False
        for k in pending:
            i, e, oid, base = items[k]
            if oid in where:
                progress = True  # duplicate: drop
                continue
            if base is not None and base not in where and (base in in_set or not allow_thin):
                still.append(k)
                continue
            seg = segments[i]
            pos = 12 + len(body)
            if e.type == OBJ_OFS_DELTA:
                body += seg[e.offset : e.offset + e.header_len]
                body += encode_ofs(pos - where[base])
                body += seg[e.data_start : e.end]
            else:
                body += seg[e.offset : e.end]
            where[oid] = pos
            if passes > 1:
                moved += 1
            progress = True
        if not progress:
            raise Unstitchable(f"{len(still)} entries can never be emitted (delta cycle or missing base)")
        pending = still
    stats = {
        "entries_in": len(items),
        "entries_out": len(where),
        "dropped_duplicates": len(items) + len(prefix) - len(where),
        "moved_after_base": moved,
        "prefix_bases": len(prefix),
        "passes": passes,
    }
    return _finish(len(where), body), stats


def describe(buf: bytes) -> dict:
    """Entry type histogram for reporting what a scenario actually contains."""
    _, entries = parse_pack(buf)
    hist: dict[str, int] = {}
    for e in entries:
        hist[TYPE_NAMES[e.type]] = hist.get(TYPE_NAMES[e.type], 0) + 1
    return {"entries": len(entries), "bytes": len(buf), "types": hist}
