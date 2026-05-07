//! Phase 39.5a PR 3: postcard-serializable wire format for
//! per-key session state.
//!
//! Mirrors a subset of the windowed module's internal types as
//! `Serialize`/`Deserialize` siblings. We don't derive serde on the
//! live types because:
//!
//! 1. `AccState::CountDistinct{Hll,Exact}*` variants hold
//!    `HyperLogLogPlus<...>` / `HashSet<...>` which don't compose
//!    cleanly with derived serde.
//! 2. We want the wire format independent from the in-memory shape
//!    so future internal refactors don't silently break recovery
//!    compatibility — version bumps go through
//!    `state_store::migrations`.
//!
//! Stateful `count_distinct` is **not** supported in PR 3; pipelines
//! configured with `state_store` + `kind = "session"` + a
//! `count_distinct` aggregator fail at config-load with a clear
//! error pointing at this limitation.
//!
//! ## On-disk shape
//!
//! ```text
//! SessionsForKeyBlob
//!   sessions: Vec<SessionStateBlob>
//!     start_ts: i64
//!     last_event_ts: i64
//!     emitted: bool
//!     dirty: bool
//!     accs: Vec<AccStateBlob>
//! ```
//!
//! `state_version = 1` for the initial PR 3 layout.

use serde::{Deserialize, Serialize};

use crate::backend::BackendError;
use crate::windowed::{AccState, GroupKey, KeyValue, SessionState};

/// Schema version for the session state-blob layout. Bumped whenever
/// `SessionsForKeyBlob` / `SessionStateBlob` / `AccStateBlob` change
/// shape; the [`crate::state_store::migrations::MigrationChain`]
/// handles upgrades on load.
pub const STATE_BLOB_VERSION: u32 = 1;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionsForKeyBlob {
    pub sessions: Vec<SessionStateBlob>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SessionStateBlob {
    pub start_ts: i64,
    pub last_event_ts: i64,
    pub emitted: bool,
    pub dirty: bool,
    pub accs: Vec<AccStateBlob>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum AccStateBlob {
    CountStar(i64),
    CountCol(i64),
    SumI64 {
        sum: i128,
        any: bool,
    },
    SumF64 {
        sum: f64,
        any: bool,
    },
    MinI64(Option<i64>),
    MinF64(Option<f64>),
    MaxI64(Option<i64>),
    MaxF64(Option<f64>),
    AvgI64 {
        sum: i128,
        count: i64,
    },
    AvgF64 {
        sum: f64,
        count: i64,
    },
    FirstI64 {
        ts: Option<i64>,
        value: Option<i64>,
    },
    FirstF64 {
        ts: Option<i64>,
        value: Option<f64>,
    },
    FirstUtf8 {
        ts: Option<i64>,
        value: Option<String>,
    },
    LastI64 {
        ts: Option<i64>,
        value: Option<i64>,
    },
    LastF64 {
        ts: Option<i64>,
        value: Option<f64>,
    },
    LastUtf8 {
        ts: Option<i64>,
        value: Option<String>,
    },
    /// Phase 39.5a P1.9: exact-mode count_distinct — the set is
    /// directly serializable. Approximate (HLL+) mode stays
    /// unsupported because `HyperLogLogPlus` keeps its register
    /// state in private fields.
    CountDistinctExactNumeric {
        values: Vec<u64>,
        cap: usize,
    },
    CountDistinctExactUtf8 {
        values: Vec<String>,
        cap: usize,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub enum KeyValueBlob {
    Null,
    Int64(i64),
    UInt64(u64),
    Float64Bits(u64),
    Utf8(String),
    TsMicros(i64),
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Hash)]
pub struct GroupKeyBlob(pub Vec<KeyValueBlob>);

// ---------------------------------------------------------------------
// AccState <-> AccStateBlob
// ---------------------------------------------------------------------

impl AccStateBlob {
    /// Borrow-only conversion from a live `AccState`. Returns
    /// `BackendError::Other` for `CountDistinctHll*` — approximate
    /// HLL+ mode keeps its register state in private fields and
    /// can't be serialized without an upstream change. Exact-mode
    /// count_distinct (P1.9) round-trips fine.
    pub fn from_state(state: &AccState) -> Result<Self, BackendError> {
        Ok(match state {
            AccState::CountStar(v) => AccStateBlob::CountStar(*v),
            AccState::CountCol(v) => AccStateBlob::CountCol(*v),
            AccState::SumI64 { sum, any } => AccStateBlob::SumI64 {
                sum: *sum,
                any: *any,
            },
            AccState::SumF64 { sum, any } => AccStateBlob::SumF64 {
                sum: *sum,
                any: *any,
            },
            AccState::MinI64(s) => AccStateBlob::MinI64(*s),
            AccState::MinF64(s) => AccStateBlob::MinF64(*s),
            AccState::MaxI64(s) => AccStateBlob::MaxI64(*s),
            AccState::MaxF64(s) => AccStateBlob::MaxF64(*s),
            AccState::AvgI64 { sum, count } => AccStateBlob::AvgI64 {
                sum: *sum,
                count: *count,
            },
            AccState::AvgF64 { sum, count } => AccStateBlob::AvgF64 {
                sum: *sum,
                count: *count,
            },
            AccState::FirstI64 { ts, value } => AccStateBlob::FirstI64 {
                ts: *ts,
                value: *value,
            },
            AccState::FirstF64 { ts, value } => AccStateBlob::FirstF64 {
                ts: *ts,
                value: *value,
            },
            AccState::FirstUtf8 { ts, value } => AccStateBlob::FirstUtf8 {
                ts: *ts,
                value: value.clone(),
            },
            AccState::LastI64 { ts, value } => AccStateBlob::LastI64 {
                ts: *ts,
                value: *value,
            },
            AccState::LastF64 { ts, value } => AccStateBlob::LastF64 {
                ts: *ts,
                value: *value,
            },
            AccState::LastUtf8 { ts, value } => AccStateBlob::LastUtf8 {
                ts: *ts,
                value: value.clone(),
            },
            AccState::CountDistinctExactNumeric { set, cap } => {
                AccStateBlob::CountDistinctExactNumeric {
                    values: set.iter().copied().collect(),
                    cap: *cap,
                }
            }
            AccState::CountDistinctExactUtf8 { set, cap } => AccStateBlob::CountDistinctExactUtf8 {
                values: set.iter().cloned().collect(),
                cap: *cap,
            },
            AccState::CountDistinctHllNumeric(_) | AccState::CountDistinctHllUtf8(_) => {
                return Err(BackendError::Other(
                    "session state persistence: count_distinct mode = \"approximate\" \
                     (HLL+) is not supported — switch to mode = \"exact\" with a \
                     bounded max_distinct_values_per_group, or drop the aggregation"
                        .into(),
                ));
            }
        })
    }

    /// Inverse of [`from_state`]. Reconstructs a live `AccState`
    /// from the wire blob. HLL+ approximate variants are
    /// unrepresentable in the blob (rejected at `from_state`), so
    /// they don't appear here.
    pub fn into_state(self) -> AccState {
        match self {
            AccStateBlob::CountStar(v) => AccState::CountStar(v),
            AccStateBlob::CountCol(v) => AccState::CountCol(v),
            AccStateBlob::SumI64 { sum, any } => AccState::SumI64 { sum, any },
            AccStateBlob::SumF64 { sum, any } => AccState::SumF64 { sum, any },
            AccStateBlob::MinI64(s) => AccState::MinI64(s),
            AccStateBlob::MinF64(s) => AccState::MinF64(s),
            AccStateBlob::MaxI64(s) => AccState::MaxI64(s),
            AccStateBlob::MaxF64(s) => AccState::MaxF64(s),
            AccStateBlob::AvgI64 { sum, count } => AccState::AvgI64 { sum, count },
            AccStateBlob::AvgF64 { sum, count } => AccState::AvgF64 { sum, count },
            AccStateBlob::FirstI64 { ts, value } => AccState::FirstI64 { ts, value },
            AccStateBlob::FirstF64 { ts, value } => AccState::FirstF64 { ts, value },
            AccStateBlob::FirstUtf8 { ts, value } => AccState::FirstUtf8 { ts, value },
            AccStateBlob::LastI64 { ts, value } => AccState::LastI64 { ts, value },
            AccStateBlob::LastF64 { ts, value } => AccState::LastF64 { ts, value },
            AccStateBlob::LastUtf8 { ts, value } => AccState::LastUtf8 { ts, value },
            AccStateBlob::CountDistinctExactNumeric { values, cap } => {
                AccState::CountDistinctExactNumeric {
                    set: values.into_iter().collect(),
                    cap,
                }
            }
            AccStateBlob::CountDistinctExactUtf8 { values, cap } => {
                AccState::CountDistinctExactUtf8 {
                    set: values.into_iter().collect(),
                    cap,
                }
            }
        }
    }
}

// ---------------------------------------------------------------------
// SessionState <-> SessionStateBlob
// ---------------------------------------------------------------------

impl SessionStateBlob {
    pub fn from_state(state: &SessionState) -> Result<Self, BackendError> {
        let accs: Result<Vec<AccStateBlob>, BackendError> =
            state.accs.iter().map(AccStateBlob::from_state).collect();
        Ok(Self {
            start_ts: state.start_ts,
            last_event_ts: state.last_event_ts,
            emitted: state.emitted,
            dirty: state.dirty,
            accs: accs?,
        })
    }

    pub fn into_state(self) -> SessionState {
        SessionState::from_blob(
            self.start_ts,
            self.last_event_ts,
            self.emitted,
            self.dirty,
            self.accs
                .into_iter()
                .map(AccStateBlob::into_state)
                .collect(),
        )
    }
}

// ---------------------------------------------------------------------
// GroupKey <-> GroupKeyBlob
// ---------------------------------------------------------------------

impl GroupKeyBlob {
    pub fn from_key(k: &GroupKey) -> Self {
        GroupKeyBlob(k.values().iter().map(KeyValueBlob::from_value).collect())
    }

    pub fn into_key(self) -> GroupKey {
        GroupKey::from_values(self.0.into_iter().map(KeyValueBlob::into_value).collect())
    }
}

impl KeyValueBlob {
    fn from_value(v: &KeyValue) -> Self {
        match v {
            KeyValue::Null => KeyValueBlob::Null,
            KeyValue::Int64(x) => KeyValueBlob::Int64(*x),
            KeyValue::UInt64(x) => KeyValueBlob::UInt64(*x),
            KeyValue::Float64Bits(x) => KeyValueBlob::Float64Bits(*x),
            KeyValue::Utf8(s) => KeyValueBlob::Utf8(s.clone()),
            KeyValue::TsMicros(x) => KeyValueBlob::TsMicros(*x),
        }
    }

    fn into_value(self) -> KeyValue {
        match self {
            KeyValueBlob::Null => KeyValue::Null,
            KeyValueBlob::Int64(x) => KeyValue::Int64(x),
            KeyValueBlob::UInt64(x) => KeyValue::UInt64(x),
            KeyValueBlob::Float64Bits(x) => KeyValue::Float64Bits(x),
            KeyValueBlob::Utf8(s) => KeyValue::Utf8(s),
            KeyValueBlob::TsMicros(x) => KeyValue::TsMicros(x),
        }
    }
}

// ---------------------------------------------------------------------
// Encode / decode
// ---------------------------------------------------------------------

/// Encode a per-key session list to postcard bytes.
pub fn encode_sessions(sessions: &[SessionState]) -> Result<Vec<u8>, BackendError> {
    let blob = SessionsForKeyBlob {
        sessions: sessions
            .iter()
            .map(SessionStateBlob::from_state)
            .collect::<Result<Vec<_>, _>>()?,
    };
    postcard::to_allocvec(&blob)
        .map_err(|e| BackendError::Other(format!("session state encode: {e}")))
}

/// Decode a per-key session list from postcard bytes.
pub fn decode_sessions(bytes: &[u8]) -> Result<Vec<SessionState>, BackendError> {
    let blob: SessionsForKeyBlob = postcard::from_bytes(bytes)
        .map_err(|e| BackendError::Other(format!("session state decode: {e}")))?;
    Ok(blob
        .sessions
        .into_iter()
        .map(SessionStateBlob::into_state)
        .collect())
}

/// Encode a `GroupKey` to opaque bytes. Postcard with the
/// `GroupKeyBlob` shape; same versioning rules as session state.
pub fn encode_group_key(k: &GroupKey) -> Result<Vec<u8>, BackendError> {
    let blob = GroupKeyBlob::from_key(k);
    postcard::to_allocvec(&blob).map_err(|e| BackendError::Other(format!("group_key encode: {e}")))
}

/// Decode a `GroupKey` from opaque bytes.
pub fn decode_group_key(bytes: &[u8]) -> Result<GroupKey, BackendError> {
    let blob: GroupKeyBlob = postcard::from_bytes(bytes)
        .map_err(|e| BackendError::Other(format!("group_key decode: {e}")))?;
    Ok(blob.into_key())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::windowed::{AccState, GroupKey, KeyValue, SessionState};

    #[test]
    fn acc_state_blob_roundtrip_count() {
        let s = AccState::CountStar(42);
        let blob = AccStateBlob::from_state(&s).unwrap();
        match blob.clone().into_state() {
            AccState::CountStar(v) => assert_eq!(v, 42),
            _ => panic!(),
        }
        // Postcard round-trip.
        let bytes = postcard::to_allocvec(&blob).unwrap();
        let back: AccStateBlob = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(blob, back);
    }

    #[test]
    fn acc_state_blob_first_utf8_roundtrip() {
        let s = AccState::FirstUtf8 {
            ts: Some(1234),
            value: Some("alice".into()),
        };
        let blob = AccStateBlob::from_state(&s).unwrap();
        let bytes = postcard::to_allocvec(&blob).unwrap();
        let back: AccStateBlob = postcard::from_bytes(&bytes).unwrap();
        assert_eq!(blob, back);
        match back.into_state() {
            AccState::FirstUtf8 { ts, value } => {
                assert_eq!(ts, Some(1234));
                assert_eq!(value, Some("alice".into()));
            }
            _ => panic!(),
        }
    }

    #[test]
    fn acc_state_blob_rejects_count_distinct_hll() {
        use hyperloglogplus::HyperLogLogPlus;
        use std::collections::hash_map::RandomState;
        let hll: HyperLogLogPlus<u64, RandomState> =
            HyperLogLogPlus::new(14, RandomState::new()).unwrap();
        let s = AccState::CountDistinctHllNumeric(hll);
        let err = AccStateBlob::from_state(&s).unwrap_err();
        assert!(err.to_string().contains("count_distinct"), "got: {err}");
        assert!(
            err.to_string().contains("exact"),
            "error should suggest exact: {err}"
        );
    }

    #[test]
    fn acc_state_blob_exact_numeric_roundtrip() {
        use std::collections::HashSet;
        let s = AccState::CountDistinctExactNumeric {
            set: [1_u64, 2, 3, 42, 99].into_iter().collect::<HashSet<_>>(),
            cap: 100,
        };
        let blob = AccStateBlob::from_state(&s).unwrap();
        let bytes = postcard::to_allocvec(&blob).unwrap();
        let back: AccStateBlob = postcard::from_bytes(&bytes).unwrap();
        match back.into_state() {
            AccState::CountDistinctExactNumeric { set, cap } => {
                assert_eq!(set.len(), 5);
                for v in [1_u64, 2, 3, 42, 99] {
                    assert!(set.contains(&v));
                }
                assert_eq!(cap, 100);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn acc_state_blob_exact_utf8_roundtrip() {
        use std::collections::HashSet;
        let s = AccState::CountDistinctExactUtf8 {
            set: ["alpha", "beta", "gamma"]
                .into_iter()
                .map(String::from)
                .collect::<HashSet<_>>(),
            cap: 50,
        };
        let blob = AccStateBlob::from_state(&s).unwrap();
        let bytes = postcard::to_allocvec(&blob).unwrap();
        let back: AccStateBlob = postcard::from_bytes(&bytes).unwrap();
        match back.into_state() {
            AccState::CountDistinctExactUtf8 { set, cap } => {
                assert_eq!(set.len(), 3);
                assert!(set.contains("alpha"));
                assert!(set.contains("beta"));
                assert!(set.contains("gamma"));
                assert_eq!(cap, 50);
            }
            _ => panic!(),
        }
    }

    #[test]
    fn session_state_blob_roundtrip() {
        let session = SessionState::from_blob(
            10,
            100,
            true,
            false,
            vec![
                AccState::CountStar(7),
                AccState::SumI64 { sum: 42, any: true },
            ],
        );
        let bytes = encode_sessions(&[session]).unwrap();
        let back = decode_sessions(&bytes).unwrap();
        assert_eq!(back.len(), 1);
        let s = &back[0];
        assert_eq!(s.start_ts, 10);
        assert_eq!(s.last_event_ts, 100);
        assert!(s.emitted);
        assert!(!s.dirty);
        assert_eq!(s.accs.len(), 2);
        match &s.accs[0] {
            AccState::CountStar(v) => assert_eq!(*v, 7),
            _ => panic!(),
        }
    }

    #[test]
    fn group_key_roundtrip_int_and_string() {
        let key = GroupKey::from_values(vec![KeyValue::Int64(123), KeyValue::Utf8("hello".into())]);
        let bytes = encode_group_key(&key).unwrap();
        let back = decode_group_key(&bytes).unwrap();
        assert_eq!(back, key);
    }

    #[test]
    fn group_key_roundtrip_null() {
        let key = GroupKey::from_values(vec![KeyValue::Null]);
        let bytes = encode_group_key(&key).unwrap();
        let back = decode_group_key(&bytes).unwrap();
        assert_eq!(back, key);
    }

    #[test]
    fn empty_sessions_list_roundtrip() {
        let bytes = encode_sessions(&[]).unwrap();
        let back = decode_sessions(&bytes).unwrap();
        assert!(back.is_empty());
    }

    /// Coverage backfill: round-trip every `AccState` variant
    /// the blob layer supports. The existing tests cover
    /// CountStar / FirstUtf8 / CountDistinctExact{Numeric,Utf8};
    /// this one fills in the remaining 12 variants so a future
    /// blob-shape change produces a loud diff per-arm.
    #[test]
    fn acc_state_blob_roundtrip_all_simple_variants() {
        let cases = vec![
            AccState::CountCol(99),
            AccState::SumI64 {
                sum: 1234,
                any: true,
            },
            AccState::SumF64 {
                sum: 12.5,
                any: true,
            },
            AccState::MinI64(Some(7)),
            AccState::MinI64(None),
            AccState::MinF64(Some(3.14)),
            AccState::MinF64(None),
            AccState::MaxI64(Some(42)),
            AccState::MaxF64(Some(99.9)),
            AccState::AvgI64 { sum: 100, count: 4 },
            AccState::AvgF64 { sum: 10.0, count: 3 },
            AccState::FirstI64 {
                ts: Some(1000),
                value: Some(7),
            },
            AccState::FirstF64 {
                ts: Some(2000),
                value: Some(2.5),
            },
            AccState::LastI64 {
                ts: Some(3000),
                value: Some(11),
            },
            AccState::LastF64 {
                ts: Some(4000),
                value: Some(99.0),
            },
            AccState::LastUtf8 {
                ts: Some(5000),
                value: Some("end".into()),
            },
            // Edge-case: First/Last with no observation yet.
            AccState::FirstI64 {
                ts: None,
                value: None,
            },
            AccState::LastUtf8 {
                ts: None,
                value: None,
            },
        ];

        for state in cases {
            let blob = AccStateBlob::from_state(&state)
                .unwrap_or_else(|e| panic!("from_state on {state:?}: {e}"));
            let bytes = postcard::to_allocvec(&blob).unwrap();
            let back: AccStateBlob = postcard::from_bytes(&bytes).unwrap();
            assert_eq!(blob, back, "postcard round-trip preserves blob shape");
            // into_state must yield the same logical state. Use
            // Debug equality as a coarse-but-honest comparator —
            // AccState doesn't implement Eq directly because of
            // the f64 / HLL variants.
            let restored = back.into_state();
            assert_eq!(
                format!("{state:?}"),
                format!("{restored:?}"),
                "into_state round-trip"
            );
        }
    }

    /// `SessionState::into_blob` + `from_blob` round-trip when the
    /// session has been emitted vs not, and dirty vs not — all
    /// four combinations exercise the boolean fields the blob
    /// layer transports verbatim.
    #[test]
    fn session_state_blob_emitted_dirty_combinations() {
        for (emitted, dirty) in [(false, false), (true, false), (false, true), (true, true)] {
            let session = SessionState::from_blob(
                100,
                500,
                emitted,
                dirty,
                vec![AccState::CountStar(1), AccState::SumF64 {
                    sum: 7.5,
                    any: true,
                }],
            );
            let bytes = encode_sessions(&[session]).unwrap();
            let back = decode_sessions(&bytes).unwrap();
            assert_eq!(back.len(), 1);
            let s = &back[0];
            assert_eq!(s.start_ts, 100);
            assert_eq!(s.last_event_ts, 500);
            assert_eq!(s.emitted, emitted, "emitted flag round-trips");
            assert_eq!(s.dirty, dirty, "dirty flag round-trips");
            assert_eq!(s.accs.len(), 2);
        }
    }

    /// `GroupKey` blob layer covers each `KeyValue` variant.
    /// The existing tests cover Int64 / Utf8 / Null; this one
    /// adds the remaining UInt64 / Float64Bits / TsMicros so
    /// every variant is round-tripped.
    #[test]
    fn group_key_roundtrip_remaining_variants() {
        let key = GroupKey::from_values(vec![
            KeyValue::UInt64(12345),
            KeyValue::Float64Bits(3.14_f64.to_bits()),
            KeyValue::TsMicros(1_700_000_000_000_000),
        ]);
        let bytes = encode_group_key(&key).unwrap();
        let back = decode_group_key(&bytes).unwrap();
        assert_eq!(back, key);
    }
}
