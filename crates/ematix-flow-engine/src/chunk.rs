//! [`DataChunk`] — a batch of same-length vectors plus a deferred row
//! [`Selection`]. The selection is the anti-materialization mechanism
//! (promoted from `ematix-flow-push::Selection`): operators narrow which
//! rows are live instead of compacting columns, so no intermediate is
//! materialized until a terminal sink.

use crate::vector::Vector;

/// Which rows of a chunk are live.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Selection {
    /// All `n` rows live (`0..n`) — the scan's no-allocation fast path.
    All(usize),
    /// Exactly these row indices, in order. May contain duplicates once
    /// a multi-match probe lands (P1).
    Indices(Vec<u32>),
}

impl Selection {
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
}

/// A chunk of decoded columns flowing through the pipeline. All `cols`
/// share one logical length; `sel` says which rows are live.
#[derive(Clone, Debug)]
pub struct DataChunk {
    pub cols: Vec<Vector>,
    pub sel: Selection,
}

impl DataChunk {
    /// A chunk over `cols` with all rows live.
    pub fn new(cols: Vec<Vector>) -> Self {
        let n = cols.first().map(Vector::len).unwrap_or(0);
        DataChunk {
            cols,
            sel: Selection::All(n),
        }
    }

    /// Underlying (pre-selection) row count of the columns.
    pub fn n_rows(&self) -> usize {
        self.cols.first().map(Vector::len).unwrap_or(0)
    }

    /// Borrow column `i`.
    pub fn col(&self, i: usize) -> &Vector {
        &self.cols[i]
    }
}
