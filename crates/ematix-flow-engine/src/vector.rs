//! The engine's owned columnar vector — the clean-room substrate.
//!
//! Design DNA is promoted from [`ematix-flow-push`]: `Arc<Vec<T>>`
//! backing so wrapping a decoded column is a move, not an element copy
//! (the measured ~8 ms/RG memcpy lesson from the PV.1 kill-gate),
//! generalized here into the engine's real format — a logical type + a
//! physical storage encoding + an optional validity bitmap.
//!
//! P0 shipped the flat encodings the Q6 spine needs (I32 `date32`, F64
//! measures); P1 adds I64 for join keys (FKs). The headline "encodings
//! survive through operators" capability — `Dictionary`, `Constant`,
//! `Sequence` — slots into [`Storage`] as later variants; added when a
//! query first needs them. See `docs/plans/NATIVE_ENGINE.md` §Arch.1.

use std::sync::Arc;

/// Logical type of a column. Kept distinct from physical [`Storage`] so a
/// date (logically `Date32`) and a raw `i32` can share I32 storage while
/// carrying different semantics to operators.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LogicalType {
    Date32,
    Float64,
    Int32,
    Int64,
}

/// Physical storage. Flat, densely-packed, `Arc`-backed buffers.
///
/// FUTURE (`docs/plans/NATIVE_ENGINE.md` §Arch.1): add
/// `Dictionary { codes, dict }`, `Constant(scalar)`, `Sequence { start,
/// step }` — the encodings that must survive *through* operators to kill
/// the dict-arrival blocker and defer materialization. Added when a query
/// first needs them, not before.
#[derive(Clone, Debug)]
pub enum Storage {
    I32(Arc<Vec<i32>>),
    I64(Arc<Vec<i64>>),
    F64(Arc<Vec<f64>>),
}

/// A typed column: logical type + physical storage + optional validity
/// (`None` = every row valid — the common required-column case).
#[derive(Clone, Debug)]
pub struct Vector {
    pub logical: LogicalType,
    pub storage: Storage,
    pub validity: Option<Arc<[bool]>>,
}

impl Vector {
    /// Wrap an `i32` buffer with an explicit logical type (e.g.
    /// `Date32`). Takes the `Vec` by move — one small `Arc` alloc, no
    /// element copy.
    pub fn i32(values: Vec<i32>, logical: LogicalType) -> Self {
        Vector {
            logical,
            storage: Storage::I32(Arc::new(values)),
            validity: None,
        }
    }

    /// Wrap an `i64` buffer (logical `Int64` — join keys / FKs).
    pub fn i64(values: Vec<i64>) -> Self {
        Vector {
            logical: LogicalType::Int64,
            storage: Storage::I64(Arc::new(values)),
            validity: None,
        }
    }

    /// Wrap an `f64` buffer (logical `Float64`).
    pub fn f64(values: Vec<f64>) -> Self {
        Vector {
            logical: LogicalType::Float64,
            storage: Storage::F64(Arc::new(values)),
            validity: None,
        }
    }

    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::I32(v) => v.len(),
            Storage::I64(v) => v.len(),
            Storage::F64(v) => v.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the whole `i32` slice. Panics if storage is not I32 (a
    /// wiring bug, not a data condition). Slice access lets a hot loop
    /// hoist the storage match out of the per-row path.
    #[inline]
    pub fn as_i32(&self) -> &[i32] {
        match &self.storage {
            Storage::I32(v) => v,
            _ => panic!("as_i32 on non-I32 vector"),
        }
    }

    /// Borrow the whole `i64` slice. Panics if storage is not I64.
    #[inline]
    pub fn as_i64(&self) -> &[i64] {
        match &self.storage {
            Storage::I64(v) => v,
            _ => panic!("as_i64 on non-I64 vector"),
        }
    }

    /// Borrow the whole `f64` slice. Panics if storage is not F64.
    #[inline]
    pub fn as_f64(&self) -> &[f64] {
        match &self.storage {
            Storage::F64(v) => v,
            _ => panic!("as_f64 on non-F64 vector"),
        }
    }
}
