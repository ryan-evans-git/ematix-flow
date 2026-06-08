//! Adaptive probe-structure selection — THE #1 design surface (ADR §3).
//!
//! Phase-0 finding 2: the same fused loop is **−16%** with a dense
//! membership structure on a bounded FK domain and **+22% (a loss)**
//! with a naive `HashSet` over the 60M-row probe side. The win is the
//! fused loop PAIRED with a domain-aware probe — so [`choose`] is
//! load-bearing, not a tuning knob.
//!
//! ## Representation note (PV.1 kill-gate finding)
//!
//! The dense structures are **byte-per-entry, indexed directly by the
//! key** (`present[key]`), NOT a packed u64 bitset. The PV.1 kill-gate
//! measured a packed bitset (bounds-check + `>>6`/`&63`/shift/mask per
//! row) at ~15ms slower over the 60M-row probe than a flat `Vec<u8>`
//! single indexed load — at SF=10/100 the probe is ALU-bound, not
//! cache-bound, so the byte array wins. Keys are non-negative (FKs);
//! `(key as usize) < len` is a single compare that also rejects
//! negatives (they wrap to a huge usize). This matches the hand-fused
//! `Vec<bool>` baseline exactly. The size gate is in ENTRIES (= bytes
//! for membership), so the dense ceiling is 8× smaller than a packed
//! bitset's — fine for SF≤100; larger domains fall to the hash table.

use std::collections::{HashMap, HashSet};

/// Dense membership (byte/entry) is chosen when the key domain is
/// non-negative and `max_key + 1 ≤ this` (32 MB). SF=10 `p_partkey`
/// max ≈ 2M (2 MB); SF=100 ≈ 20M (20 MB) — both fit. Larger → HashSet.
pub const DENSE_BITSET_MAX_DOMAIN: i128 = 32_000_000;

/// Dense payload (i64 slot, 8 B) is chosen when `max_key + 1 ≤ this`
/// (32 MB / 8 = 4M slots). SF=10 `s_suppkey` ≈ 100K fits; `o_orderkey`
/// ≈ 15M (sparse to ~6×) does NOT → hash table, exactly as the Phase-0
/// prototype used a `HashMap` for orders.
pub const DENSE_PAYLOAD_MAX_DOMAIN: i128 = 4_000_000;

/// A frozen build side, queried once per probe row. Built by [`choose`].
pub enum ProbeStructure {
    /// Membership, byte-per-entry indexed by key (`present[key] != 0`).
    DenseSet { present: Vec<u8> },
    /// Membership + one i64 payload per key (unique build), indexed by key.
    DensePayload { present: Vec<u8>, payload: Vec<i64> },
    /// Membership-only fallback (negative / huge domain).
    HashSet(HashSet<i64>),
    /// Payload fallback, unique build (negative / huge domain).
    HashTable(HashMap<i64, i64>),
    /// Payload, NON-unique build — enumerates all matches per key (C4).
    HashMultiTable(HashMap<i64, Vec<i64>>),
}

impl ProbeStructure {
    /// Does `key` match at least one build row? Hot path — one indexed
    /// load for the dense variants; `(key as usize) < len` also rejects
    /// negative keys (they wrap high).
    #[inline]
    pub fn contains(&self, key: i64) -> bool {
        match self {
            ProbeStructure::DenseSet { present } => {
                (key as usize) < present.len() && present[key as usize] != 0
            }
            ProbeStructure::DensePayload { present, .. } => {
                (key as usize) < present.len() && present[key as usize] != 0
            }
            ProbeStructure::HashSet(s) => s.contains(&key),
            ProbeStructure::HashTable(m) => m.contains_key(&key),
            ProbeStructure::HashMultiTable(m) => m.contains_key(&key),
        }
    }

    /// The payload of the FIRST match for `key` (or `None`). `Some(0)`
    /// for membership-only structures when present. A terminal sink
    /// uses this to recover a build column for surviving rows AND to
    /// test inner-join membership in one lookup.
    #[inline]
    pub fn payload(&self, key: i64) -> Option<i64> {
        match self {
            ProbeStructure::DenseSet { present } => {
                if (key as usize) < present.len() && present[key as usize] != 0 {
                    Some(0)
                } else {
                    None
                }
            }
            ProbeStructure::DensePayload { present, payload } => {
                let k = key as usize;
                if k < present.len() && present[k] != 0 {
                    Some(payload[k])
                } else {
                    None
                }
            }
            ProbeStructure::HashSet(s) => s.contains(&key).then_some(0),
            ProbeStructure::HashTable(m) => m.get(&key).copied(),
            ProbeStructure::HashMultiTable(m) => m.get(&key).and_then(|v| v.first().copied()),
        }
    }

    /// Invoke `f(payload)` once per matching build row (0/1 for a unique
    /// build; 0..N for multi-match). The inner-join emit primitive (C4).
    #[inline]
    pub fn for_matches(&self, key: i64, mut f: impl FnMut(i64)) {
        match self {
            ProbeStructure::HashMultiTable(m) => {
                if let Some(v) = m.get(&key) {
                    for &p in v {
                        f(p);
                    }
                }
            }
            // All other variants match ≤1 row.
            _ => {
                if let Some(p) = self.payload(key) {
                    f(p);
                }
            }
        }
    }

    /// True only for builds that can match a probe row more than once
    /// (non-unique). Lets `ProbePush` take the closure-free `contains`
    /// fast path for the common unique/membership case.
    #[inline]
    pub fn may_multi_match(&self) -> bool {
        matches!(self, ProbeStructure::HashMultiTable(_))
    }

    /// Human-readable variant tag, for the kill-gate's probe-mode log.
    pub fn kind(&self) -> &'static str {
        match self {
            ProbeStructure::DenseSet { .. } => "DenseSet",
            ProbeStructure::DensePayload { .. } => "DensePayload",
            ProbeStructure::HashSet(_) => "HashSet",
            ProbeStructure::HashTable(_) => "HashTable",
            ProbeStructure::HashMultiTable(_) => "HashMultiTable",
        }
    }
}

/// Pick + build the probe structure for a collected build side (ADR §3.2).
///
/// - `keys`: the build-side join-key values (already filtered).
/// - `payloads`: `None` for a semi/membership join; `Some(p)` (same
///   length as `keys`) to carry a per-key payload (e.g. orders→year).
///
/// Densify (byte set / payload) iff the domain is **non-negative** and
/// `max + 1` fits the size gate; non-unique + payload → multi-table;
/// otherwise hash fallback. The fallback is always correct —
/// densification is a pure speedup.
pub fn choose(keys: &[i64], payloads: Option<&[i64]>) -> ProbeStructure {
    if keys.is_empty() {
        return match payloads {
            None => ProbeStructure::HashSet(HashSet::new()),
            Some(_) => ProbeStructure::HashTable(HashMap::new()),
        };
    }
    let mut lo = keys[0];
    let mut hi = keys[0];
    for &k in keys {
        if k < lo {
            lo = k;
        }
        if k > hi {
            hi = k;
        }
    }
    // Byte/payload arrays are indexed directly by key from 0, so the
    // domain must be non-negative; `hi + 1` is the array length.
    let densifiable = lo >= 0;
    let len = (hi as i128) + 1;

    match payloads {
        // ---- membership-only (semi-join) ----
        None => {
            if densifiable && len <= DENSE_BITSET_MAX_DOMAIN {
                let mut present = vec![0u8; len as usize];
                for &k in keys {
                    present[k as usize] = 1;
                }
                ProbeStructure::DenseSet { present }
            } else {
                ProbeStructure::HashSet(keys.iter().copied().collect())
            }
        }
        // ---- payload (inner join carrying a build column) ----
        // Duplicate detection (→ C4 multi-table) is folded INTO the
        // build pass (present-bit for dense, insert-return for hash),
        // so the common unique build pays no extra pass.
        Some(p) => {
            debug_assert_eq!(p.len(), keys.len(), "payloads must align with keys");
            if densifiable && len <= DENSE_PAYLOAD_MAX_DOMAIN {
                let n = len as usize;
                let mut present = vec![0u8; n];
                let mut payload = vec![0i64; n];
                let mut dup = false;
                for (j, &k) in keys.iter().enumerate() {
                    let idx = k as usize;
                    if present[idx] != 0 {
                        dup = true;
                        break;
                    }
                    present[idx] = 1;
                    payload[idx] = p[j];
                }
                if !dup {
                    return ProbeStructure::DensePayload { present, payload };
                }
            } else {
                let mut m = HashMap::with_capacity(keys.len());
                let mut dup = false;
                for (j, &k) in keys.iter().enumerate() {
                    if m.insert(k, p[j]).is_some() {
                        dup = true;
                        break;
                    }
                }
                if !dup {
                    return ProbeStructure::HashTable(m);
                }
            }
            // Non-unique build → enumerate all matches (C4).
            let mut mm: HashMap<i64, Vec<i64>> = HashMap::new();
            for (j, &k) in keys.iter().enumerate() {
                mm.entry(k).or_default().push(p[j]);
            }
            ProbeStructure::HashMultiTable(mm)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bounded_membership_picks_dense_set() {
        let ps = choose(&[1, 5, 100, 2_000_000], None);
        assert_eq!(ps.kind(), "DenseSet");
        assert!(ps.contains(1) && ps.contains(2_000_000) && ps.contains(100));
        assert!(!ps.contains(2) && !ps.contains(2_000_001) && !ps.contains(0));
        assert!(!ps.contains(-1), "negative key must not match (wraps high)");
    }

    #[test]
    fn bounded_payload_unique_picks_dense_payload() {
        let ps = choose(&[10, 20, 30], Some(&[1995, 1996, 1995]));
        assert_eq!(ps.kind(), "DensePayload");
        assert_eq!(ps.payload(20), Some(1996));
        assert_eq!(ps.payload(99), None);
    }

    #[test]
    fn huge_domain_membership_falls_back_to_hashset() {
        let ps = choose(&[1, 99_000_000], None); // > 32M gate
        assert_eq!(ps.kind(), "HashSet");
        assert!(ps.contains(99_000_000) && ps.contains(1) && !ps.contains(2));
    }

    #[test]
    fn huge_domain_payload_falls_back_to_hashtable() {
        let ps = choose(&[1, 9_000_000], Some(&[7, 8])); // > 4M gate
        assert_eq!(ps.kind(), "HashTable");
        assert_eq!(ps.payload(9_000_000), Some(8));
    }

    #[test]
    fn negative_keys_fall_back_to_hash() {
        // Byte arrays index from 0, so negatives can't densify → hash.
        let ps = choose(&[-5, -1, 3], None);
        assert_eq!(ps.kind(), "HashSet");
        assert!(ps.contains(-5) && ps.contains(3) && !ps.contains(0));
    }

    #[test]
    fn duplicate_keys_with_payload_pick_multitable_and_enumerate() {
        // C4 multi-match: key 5 twice with different payloads.
        let ps = choose(&[5, 5, 9], Some(&[100, 200, 300]));
        assert_eq!(ps.kind(), "HashMultiTable");
        assert!(ps.may_multi_match());
        let mut payloads = Vec::new();
        ps.for_matches(5, |p| payloads.push(p));
        payloads.sort();
        assert_eq!(payloads, vec![100, 200], "both matches must be enumerated");
        let mut once = 0;
        ps.for_matches(9, |_| once += 1);
        assert_eq!(once, 1);
    }

    #[test]
    fn empty_build_is_correct() {
        let ps = choose(&[], None);
        assert!(!ps.contains(1));
        let ps = choose(&[], Some(&[]));
        assert_eq!(ps.payload(1), None);
    }
}
