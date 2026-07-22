//! Spill primitives: a temp-dir-backed store of fixed-width `(i64, i64)`
//! records, one append-only file per partition.
//!
//! This is the foundation for breakers that must stay correct beyond a
//! memory budget — the spilling aggregate ([`crate::agg`]) today, the
//! hash-join build next. Records are 16 raw little-endian bytes, so a
//! round-trip is bit-identical and the read-back streams (one partition's
//! file at a time, never the whole spill set). Std-only: no `tempfile`,
//! no third-party dep, so the engine stays clean-room.
//!
//! RAII: the temp dir is created lazily on the first real spill and
//! removed on drop, so an aggregate that fits in memory pays no
//! filesystem cost at all.

use std::fs::{self, File};
use std::io::{self, BufReader, BufWriter, Read, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

/// Process-wide sequence so concurrent spills get distinct temp dirs.
static SPILL_SEQ: AtomicU64 = AtomicU64::new(0);

/// Bytes per spilled record: i64 key + i64 value, little-endian.
const REC_BYTES: usize = 16;

/// Route a key to one of `2^part_bits` partitions via Fibonacci hashing —
/// the top `part_bits` bits of the multiplicative hash (its best-mixed
/// bits). Every partitioned breaker routes through this one function, so a
/// key's rows always co-locate: both sides of a join land in the same
/// partition, and the aggregate keeps a key whole. Requires
/// `1 <= part_bits <= 63`.
#[inline]
pub fn part_of(key: i64, part_bits: u32) -> usize {
    let h = (key as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (h >> (64 - part_bits)) as usize
}

/// A partitioned, disk-backed overflow store. One append-only file per
/// partition; group-locality is the caller's contract (all rows of a key
/// route to one partition), so a partition can later be aggregated
/// independently.
pub struct PartitionSpill {
    dir: PathBuf,
    /// Whether `dir` has actually been created (lazy — see module docs).
    created: bool,
    /// Per-partition append writer, opened on that partition's first spill.
    writers: Vec<Option<BufWriter<File>>>,
    spilled_records: u64,
}

impl PartitionSpill {
    /// A store for `npart` partitions. Infallible — no filesystem touch
    /// until the first [`append`](Self::append).
    pub fn new(npart: usize) -> Self {
        let seq = SPILL_SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("ematix_spill_{}_{}", std::process::id(), seq));
        let mut writers = Vec::with_capacity(npart);
        writers.resize_with(npart, || None);
        Self {
            dir,
            created: false,
            writers,
            spilled_records: 0,
        }
    }

    fn path(&self, p: usize) -> PathBuf {
        self.dir.join(format!("p{p}.bin"))
    }

    fn ensure_dir(&mut self) -> io::Result<()> {
        if !self.created {
            fs::create_dir_all(&self.dir)?;
            self.created = true;
        }
        Ok(())
    }

    /// Append the `(keys[i], vals[i])` records (equal-length slices) to
    /// partition `p`'s file, creating the temp dir / file on first use.
    pub fn append(&mut self, p: usize, keys: &[i64], vals: &[i64]) -> io::Result<()> {
        debug_assert_eq!(keys.len(), vals.len());
        if keys.is_empty() {
            return Ok(());
        }
        if self.writers[p].is_none() {
            self.ensure_dir()?;
            let f = File::create(self.path(p))?;
            self.writers[p] = Some(BufWriter::new(f));
        }
        let w = self.writers[p].as_mut().expect("writer just ensured");
        let mut rec = [0u8; REC_BYTES];
        for (k, v) in keys.iter().zip(vals) {
            rec[..8].copy_from_slice(&k.to_le_bytes());
            rec[8..].copy_from_slice(&v.to_le_bytes());
            w.write_all(&rec)?;
        }
        self.spilled_records += keys.len() as u64;
        Ok(())
    }

    /// Whether partition `p` has any spilled records.
    pub fn has_spill(&self, p: usize) -> bool {
        self.writers[p].is_some()
    }

    /// Stream every spilled record of partition `p` back in append order,
    /// calling `f(key, val)`. Flushes the partition's writer first and
    /// leaves the partition drained (its writer taken). Streams via a
    /// `BufReader`, so peak memory is one record, not the whole run.
    pub fn drain_partition(&mut self, p: usize, mut f: impl FnMut(i64, i64)) -> io::Result<()> {
        let Some(mut w) = self.writers[p].take() else {
            return Ok(());
        };
        w.flush()?;
        drop(w); // close the write handle before re-opening for read.

        let mut r = BufReader::new(File::open(self.path(p))?);
        let mut rec = [0u8; REC_BYTES];
        loop {
            match r.read_exact(&mut rec) {
                Ok(()) => {
                    let k = i64::from_le_bytes(rec[..8].try_into().unwrap());
                    let v = i64::from_le_bytes(rec[8..].try_into().unwrap());
                    f(k, v);
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    /// Total records spilled to disk over this store's lifetime.
    pub fn spilled_records(&self) -> u64 {
        self.spilled_records
    }
}

impl Drop for PartitionSpill {
    fn drop(&mut self) {
        // Best-effort temp cleanup; nothing correctness-relevant depends on
        // it succeeding (the OS reclaims temp on reboot regardless).
        if self.created {
            let _ = fs::remove_dir_all(&self.dir);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_records_per_partition_in_append_order() {
        let mut s = PartitionSpill::new(4);
        assert!(!s.has_spill(1));

        // Two appends to partition 1, one to partition 3.
        s.append(1, &[10, 11], &[100, 110]).unwrap();
        s.append(1, &[12], &[120]).unwrap();
        s.append(3, &[30], &[300]).unwrap();
        assert!(s.has_spill(1) && s.has_spill(3) && !s.has_spill(0));
        assert_eq!(s.spilled_records(), 4);

        let mut p1 = Vec::new();
        s.drain_partition(1, |k, v| p1.push((k, v))).unwrap();
        assert_eq!(
            p1,
            vec![(10, 100), (11, 110), (12, 120)],
            "append order preserved"
        );

        let mut p3 = Vec::new();
        s.drain_partition(3, |k, v| p3.push((k, v))).unwrap();
        assert_eq!(p3, vec![(30, 300)]);

        // Draining a never-spilled partition yields nothing, no error.
        let mut p0 = Vec::new();
        s.drain_partition(0, |k, v| p0.push((k, v))).unwrap();
        assert!(p0.is_empty());
    }

    #[test]
    fn empty_append_is_a_noop() {
        let mut s = PartitionSpill::new(2);
        s.append(0, &[], &[]).unwrap();
        assert!(!s.has_spill(0));
        assert_eq!(s.spilled_records(), 0);
    }

    #[test]
    fn part_of_stays_in_range() {
        for bits in 1..=16u32 {
            let npart = 1usize << bits;
            for key in [-9_i64, -1, 0, 1, 2, 7, 1234, i64::MAX, i64::MIN] {
                assert!(part_of(key, bits) < npart);
            }
        }
    }
}
