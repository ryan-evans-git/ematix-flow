//! The morsel envelope: decoded columns + a deferred row selection.
//!
//! The selection is the mechanism that defers the `take`: a
//! `ProbePush`/`FilterPush` narrows `sel` instead of gathering, so
//! columns are never compacted until the terminal sink. Columns are
//! `Arc`-backed so `ProjectPush` (and morsel cloning across operators)
//! is a pointer copy, never a data copy.

use std::sync::Arc;

/// Which rows of a morsel are live. `All(n)` is the no-allocation fast
/// path produced by a scan; `Indices` is produced once a filter/probe
/// narrows it. Downstream operators read columns THROUGH this.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selection {
    /// All `n` rows are live (rows `0..n`).
    All(usize),
    /// Exactly these row indices are live, in order. May contain
    /// duplicates (a multi-match probe emits one entry per match — C4).
    Indices(Vec<u32>),
}

impl Selection {
    /// Number of live rows.
    pub fn len(&self) -> usize {
        match self {
            Selection::All(n) => *n,
            Selection::Indices(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    /// Visit each live row index in order.
    #[inline]
    pub fn for_each(&self, mut f: impl FnMut(u32)) {
        match self {
            Selection::All(n) => {
                for i in 0..*n as u32 {
                    f(i);
                }
            }
            Selection::Indices(v) => {
                for &i in v {
                    f(i);
                }
            }
        }
    }
    /// Materialise the live indices (used by terminal compaction).
    pub fn to_indices(&self) -> Vec<u32> {
        match self {
            Selection::All(n) => (0..*n as u32).collect(),
            Selection::Indices(v) => v.clone(),
        }
    }
}

/// Typed, dense column payload. Pure — no Arrow. Stored as
/// `Arc<Vec<T>>` so wrapping a decoded `Vec` is a move (one small Arc
/// alloc), NOT an element copy — `Arc<[T]>::from(Vec)` would memcpy the
/// whole column (measured ~8ms/RG-sweep in the PV.1 kill-gate). PV.2's
/// glue wraps already-`Arc`'d Arrow buffers the same way (no copy).
#[derive(Clone, Debug)]
pub enum ColumnData {
    I64(Arc<Vec<i64>>),
    F64(Arc<Vec<f64>>),
}

/// A decoded column with optional validity. `validity == None` means
/// every row is non-null (the common FK/measure case). When present,
/// `validity[i] == false` marks row `i` NULL — and a NULL key MUST
/// never match an equi-join (C1; the scar that left Q07 sums 94%
/// wrong for months was a null-handling miss masked by row-count-only
/// checks).
#[derive(Clone, Debug)]
pub struct Column {
    pub data: ColumnData,
    pub validity: Option<Arc<[bool]>>,
}

impl Column {
    pub fn i64(values: Vec<i64>) -> Self {
        Column {
            data: ColumnData::I64(Arc::new(values)),
            validity: None,
        }
    }
    pub fn f64(values: Vec<f64>) -> Self {
        Column {
            data: ColumnData::F64(Arc::new(values)),
            validity: None,
        }
    }
    /// Attach a validity bitmap (`true` = valid).
    pub fn with_validity(mut self, valid: impl Into<Arc<[bool]>>) -> Self {
        self.validity = Some(valid.into());
        self
    }

    pub fn len(&self) -> usize {
        match &self.data {
            ColumnData::I64(v) => v.len(),
            ColumnData::F64(v) => v.len(),
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Borrow the validity bitmap (`true` = valid), or `None` if every
    /// row is valid. Lets a hot loop hoist the null check out of the
    /// per-row path.
    #[inline]
    pub fn validity_slice(&self) -> Option<&[bool]> {
        self.validity.as_deref()
    }

    #[inline]
    fn is_valid(&self, i: usize) -> bool {
        match &self.validity {
            None => true,
            Some(v) => v[i],
        }
    }

    /// The i64 key at row `i`, or `None` if the row is NULL. Panics if
    /// the column is not I64 (a wiring bug, not a data condition).
    #[inline]
    pub fn key_i64(&self, i: usize) -> Option<i64> {
        if !self.is_valid(i) {
            return None;
        }
        match &self.data {
            ColumnData::I64(v) => Some(v[i]),
            ColumnData::F64(_) => panic!("key_i64 on F64 column"),
        }
    }

    /// The raw i64 at row `i` ignoring validity (for non-key reads).
    #[inline]
    pub fn i64_at(&self, i: usize) -> i64 {
        match &self.data {
            ColumnData::I64(v) => v[i],
            ColumnData::F64(_) => panic!("i64_at on F64 column"),
        }
    }
    #[inline]
    pub fn f64_at(&self, i: usize) -> f64 {
        match &self.data {
            ColumnData::F64(v) => v[i],
            ColumnData::I64(_) => panic!("f64_at on I64 column"),
        }
    }
}

/// A chunk of decoded columns flowing through the pipeline. `cols` are
/// all the same logical length; `sel` says which rows are live.
/// `row_base` is the absolute offset of row 0 within the source
/// partition (provenance for any breaker needing stable numbering).
#[derive(Clone, Debug)]
pub struct Morsel {
    pub cols: Vec<Column>,
    pub sel: Selection,
    pub row_base: u64,
}

impl Morsel {
    /// A morsel over `cols` with all rows live.
    pub fn new(cols: Vec<Column>) -> Self {
        let n = cols.first().map(|c| c.len()).unwrap_or(0);
        Morsel {
            cols,
            sel: Selection::All(n),
            row_base: 0,
        }
    }
    pub fn with_row_base(mut self, base: u64) -> Self {
        self.row_base = base;
        self
    }
    /// Underlying (pre-selection) row count of the columns.
    pub fn n_rows(&self) -> usize {
        self.cols.first().map(|c| c.len()).unwrap_or(0)
    }
}
