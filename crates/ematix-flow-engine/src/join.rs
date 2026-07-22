//! Join primitives.
//!
//! The adaptive probe structure is reused from `ematix-flow-push`
//! (DF-free, Arrow-free — the proven dense/hash selector: a bounded
//! non-negative FK domain densifies to a byte-indexed set/payload, else
//! falls to a hash table). The engine adds `probe_narrow`: the semi-join
//! membership operator over the engine's `DataChunk` that narrows the
//! deferred selection **without materializing** the surviving rows — the
//! mechanism that beats the pull model's per-join `take` (PV.1: −16% on
//! the 60M-row Q08 probe).

pub use ematix_flow_push::{ProbeStructure, choose};

use crate::chunk::{DataChunk, Selection};

/// Narrow `chunk`'s current selection to the rows whose i64 key (column
/// `key_col`) is a member of `probe`. Semi-join hot path: a closure-free
/// `contains` loop over the live rows, emitting a new `Selection` — the
/// columns are never compacted. Assumes non-null keys (the FK case P1
/// decodes); a validity-aware path is added when a nullable key first
/// needs it.
pub fn probe_narrow(chunk: &DataChunk, key_col: usize, probe: &ProbeStructure) -> Selection {
    let key = chunk.col(key_col).as_i64();
    let mut out: Vec<u32> = Vec::with_capacity(chunk.sel.len());
    chunk.sel.for_each(|i| {
        if probe.contains(key[i as usize]) {
            out.push(i);
        }
    });
    Selection::Indices(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::vector::Vector;

    #[test]
    fn probe_narrow_keeps_only_members() {
        // col0 = probe keys; keep only those in {2, 4}.
        let chunk = DataChunk::new(vec![Vector::i64(vec![1, 2, 3, 4, 5])]);
        let ps = choose(&[2, 4], None);
        let sel = probe_narrow(&chunk, 0, &ps);
        // surviving row indices are 1 (key 2) and 3 (key 4).
        assert_eq!(sel, Selection::Indices(vec![1, 3]));
    }

    #[test]
    fn probe_narrow_composes_over_an_existing_selection() {
        // Pre-narrow to rows {0,1,2} (drop row 3), then probe {2,4}: only
        // key 2 (row 1) survives — row 3's key 4 was already excluded.
        let mut chunk = DataChunk::new(vec![Vector::i64(vec![1, 2, 3, 4])]);
        chunk.sel = Selection::Indices(vec![0, 1, 2]);
        let ps = choose(&[2, 4], None);
        let sel = probe_narrow(&chunk, 0, &ps);
        assert_eq!(sel, Selection::Indices(vec![1]));
    }

    #[test]
    fn probe_narrow_empty_when_no_members() {
        let chunk = DataChunk::new(vec![Vector::i64(vec![1, 2, 3])]);
        let ps = choose(&[99], None);
        let sel = probe_narrow(&chunk, 0, &ps);
        assert!(sel.is_empty());
    }
}
