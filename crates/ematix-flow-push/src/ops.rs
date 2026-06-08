//! Push operators: narrow the selection (or attach a probe payload)
//! and call the downstream inline. None compacts — the deferred
//! `take` happens once, at the terminal sink.

use std::sync::Arc;

use crate::PushError;
use crate::morsel::{Column, Morsel, Selection};
use crate::probe::ProbeStructure;

/// A pipelined or breaker operator. Pipelined ops narrow/rebind and
/// `out.emit`; breakers absorb in `push` and flush in `finish`.
pub trait PushOperator: Send {
    fn push(&mut self, m: Morsel, out: &mut dyn Sink) -> Result<(), PushError>;
    fn finish(&mut self, _out: &mut dyn Sink) -> Result<(), PushError> {
        Ok(())
    }
}

/// The downstream of an operator.
pub trait Sink {
    fn emit(&mut self, m: Morsel) -> Result<(), PushError>;
}

/// A row predicate for [`FilterPush`]. Minimal set for PV.1 (range/eq
/// on the decoded fact columns). NULL fails every predicate.
#[derive(Clone, Debug)]
pub enum Pred {
    I64Range { lo: i64, hi: i64 },
    I64Eq(i64),
    F64Range { lo: f64, hi: f64 },
}

impl Pred {
    #[inline]
    fn eval(&self, col: &Column, i: usize) -> bool {
        match self {
            Pred::I64Range { lo, hi } => match col.key_i64(i) {
                Some(v) => v >= *lo && v <= *hi,
                None => false,
            },
            Pred::I64Eq(x) => col.key_i64(i) == Some(*x),
            Pred::F64Range { lo, hi } => {
                // f64 columns carry no validity in PV.1; treat all valid.
                let v = col.f64_at(i);
                v >= *lo && v <= *hi
            }
        }
    }
}

/// Narrows the selection by a predicate on one column. Pipelined.
pub struct FilterPush {
    pub col: usize,
    pub pred: Pred,
}

impl PushOperator for FilterPush {
    fn push(&mut self, m: Morsel, out: &mut dyn Sink) -> Result<(), PushError> {
        if self.col >= m.cols.len() {
            return Err(PushError::BadColumn(self.col));
        }
        let mut idx: Vec<u32> = Vec::new();
        {
            let c = &m.cols[self.col];
            m.sel.for_each(|i| {
                if self.pred.eval(c, i as usize) {
                    idx.push(i);
                }
            });
        }
        out.emit(Morsel {
            cols: m.cols,
            sel: Selection::Indices(idx),
            row_base: m.row_base,
        })
    }
}

/// Rebinds the column set to a projected subset by explicit index
/// (C3: never relies on scan order; the output mapping is explicit).
/// Selection is preserved. Column clones are `Arc` pointer copies.
pub struct ProjectPush {
    pub indices: Vec<usize>,
}

impl PushOperator for ProjectPush {
    fn push(&mut self, m: Morsel, out: &mut dyn Sink) -> Result<(), PushError> {
        let mut cols = Vec::with_capacity(self.indices.len());
        for &i in &self.indices {
            let c = m.cols.get(i).ok_or(PushError::BadColumn(i))?;
            cols.push(c.clone());
        }
        out.emit(Morsel {
            cols,
            sel: m.sel,
            row_base: m.row_base,
        })
    }
}

/// The heart: tests each live key against a frozen [`ProbeStructure`],
/// narrows the selection to inner-join matches, and (when `attach`)
/// appends an i64 payload column carrying the matched build value.
///
/// - A NULL key never matches (C1).
/// - `attach` requires a UNIQUE build (the payload column is
///   original-row-aligned, so a row may match at most once). The
///   Q08-shape builds are dimension PKs (unique). Non-unique +
///   attach is rejected (`debug_assert`) — Phase 2 territory.
/// - Membership-only / narrow mode (`attach == false`) supports
///   multi-match: a probe row matching N build rows is emitted N
///   times (C4).
pub struct ProbePush {
    pub key_col: usize,
    pub probe: Arc<ProbeStructure>,
    pub attach: bool,
}

impl ProbePush {
    pub fn narrow(key_col: usize, probe: Arc<ProbeStructure>) -> Self {
        ProbePush {
            key_col,
            probe,
            attach: false,
        }
    }
    pub fn attach(key_col: usize, probe: Arc<ProbeStructure>) -> Self {
        debug_assert!(
            !matches!(*probe, ProbeStructure::HashMultiTable(_)),
            "attach requires a unique build; non-unique payload join is Phase 2"
        );
        ProbePush {
            key_col,
            probe,
            attach: true,
        }
    }
}

impl PushOperator for ProbePush {
    fn push(&mut self, m: Morsel, out: &mut dyn Sink) -> Result<(), PushError> {
        if self.key_col >= m.cols.len() {
            return Err(PushError::BadColumn(self.key_col));
        }
        let n = m.n_rows();
        // Pre-size the survivor buffer to the live count to avoid growth
        // churn over the (up to 60M-row) probe pass.
        let mut new_idx: Vec<u32> = Vec::with_capacity(m.sel.len());
        let mut payload_col: Option<Vec<i64>> = if self.attach {
            Some(vec![0i64; n])
        } else {
            None
        };
        {
            let key = &m.cols[self.key_col];
            let probe = &*self.probe;
            if !self.attach && !probe.may_multi_match() {
                // Hot path: membership narrow against a ≤1-match probe.
                // A tight, closure-free `contains` loop — the per-row
                // `for_matches` closure + packed-bitset extraction were
                // the abstraction tax over the 60M-row probe (PV.1 kill-gate).
                match key.validity_slice() {
                    None => {
                        // No nulls (the FK case): branch-free key read.
                        m.sel.for_each(|i| {
                            if probe.contains(key.i64_at(i as usize)) {
                                new_idx.push(i);
                            }
                        });
                    }
                    Some(valid) => {
                        m.sel.for_each(|i| {
                            let iu = i as usize;
                            if valid[iu] && probe.contains(key.i64_at(iu)) {
                                new_idx.push(i);
                            }
                        });
                    }
                }
            } else {
                // Attach path (unique build): one payload write per match.
                m.sel.for_each(|i| {
                    if let Some(k) = key.key_i64(i as usize) {
                        probe.for_matches(k, |p| {
                            new_idx.push(i);
                            if let Some(pc) = payload_col.as_mut() {
                                pc[i as usize] = p;
                            }
                        });
                    }
                });
            }
        }
        let mut cols = m.cols;
        if let Some(pc) = payload_col {
            cols.push(Column::i64(pc));
        }
        out.emit(Morsel {
            cols,
            sel: Selection::Indices(new_idx),
            row_base: m.row_base,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::probe::choose;
    use crate::run_pipeline;

    /// Test sink: gathers live values of column `col` (i64) through the
    /// final selection — i.e. performs the single terminal compaction.
    #[derive(Default)]
    struct CollectI64 {
        col: usize,
        out: Vec<i64>,
    }
    impl Sink for CollectI64 {
        fn emit(&mut self, m: Morsel) -> Result<(), PushError> {
            let c = &m.cols[self.col];
            m.sel.for_each(|i| self.out.push(c.i64_at(i as usize)));
            Ok(())
        }
    }

    fn morsel_i64(cols: Vec<Vec<i64>>) -> Morsel {
        Morsel::new(cols.into_iter().map(Column::i64).collect())
    }

    #[test]
    fn probe_narrow_filters_to_members() {
        // col0 = probe keys; keep only those in {2,4}.
        let m = morsel_i64(vec![vec![1, 2, 3, 4, 5]]);
        let ps = Arc::new(choose(&[2, 4], None));
        let mut ops: Vec<Box<dyn PushOperator>> = vec![Box::new(ProbePush::narrow(0, ps))];
        let mut sink = CollectI64 {
            col: 0,
            out: vec![],
        };
        run_pipeline(&mut ops, [m], &mut sink).unwrap();
        assert_eq!(sink.out, vec![2, 4]);
    }

    #[test]
    fn null_key_never_matches() {
        // C1: row 2's key is NULL even though value 3 is "in" the set.
        let keys = Column::i64(vec![1, 2, 3, 4]).with_validity(vec![true, true, false, true]);
        let m = Morsel::new(vec![keys]);
        let ps = Arc::new(choose(&[3, 4], None));
        let mut ops: Vec<Box<dyn PushOperator>> = vec![Box::new(ProbePush::narrow(0, ps))];
        let mut sink = CollectI64 {
            col: 0,
            out: vec![],
        };
        run_pipeline(&mut ops, [m], &mut sink).unwrap();
        assert_eq!(
            sink.out,
            vec![4],
            "NULL key (3) must not match despite being in the set"
        );
    }

    #[test]
    fn probe_attach_payload_aligns_with_survivors() {
        // keys col0; payload-attach orders→year; sum check downstream.
        let m = morsel_i64(vec![vec![10, 20, 30, 40], vec![100, 200, 300, 400]]);
        let ps = Arc::new(choose(&[20, 40], Some(&[1995, 1996])));
        let mut ops: Vec<Box<dyn PushOperator>> = vec![Box::new(ProbePush::attach(0, ps))];
        // attached payload is appended as col index 2.
        let mut sink = CollectI64 {
            col: 2,
            out: vec![],
        };
        run_pipeline(&mut ops, [m], &mut sink).unwrap();
        assert_eq!(sink.out, vec![1995, 1996]);
    }

    #[test]
    fn project_permutes_columns_by_explicit_index() {
        // C3: reorder [a,b,c] -> [c,a]; values read through projection.
        let m = morsel_i64(vec![vec![1, 2], vec![10, 20], vec![100, 200]]);
        let mut ops: Vec<Box<dyn PushOperator>> = vec![Box::new(ProjectPush {
            indices: vec![2, 0],
        })];
        let mut sink = CollectI64 {
            col: 0,
            out: vec![],
        }; // col0 of projection = original col2
        run_pipeline(&mut ops, [m], &mut sink).unwrap();
        assert_eq!(sink.out, vec![100, 200]);
    }

    #[test]
    fn multi_match_narrow_emits_one_row_per_match() {
        // C4: build has key 5 twice; probe row with key 5 emits twice.
        let m = morsel_i64(vec![vec![5, 9, 7]]);
        let ps = Arc::new(choose(&[5, 5, 9], Some(&[0, 0, 0]))); // HashMultiTable
        let mut ops: Vec<Box<dyn PushOperator>> = vec![Box::new(ProbePush::narrow(0, ps))];
        let mut sink = CollectI64 {
            col: 0,
            out: vec![],
        };
        run_pipeline(&mut ops, [m], &mut sink).unwrap();
        // key 5 matches twice → emitted twice; key 9 once; key 7 absent.
        assert_eq!(sink.out, vec![5, 5, 9]);
    }

    #[test]
    fn filter_then_probe_chain_composes() {
        // FilterPush (keep l<=3) then ProbePush (keep in {2,3,9}).
        let m = morsel_i64(vec![vec![1, 2, 3, 4], vec![10, 20, 30, 40]]);
        let ps = Arc::new(choose(&[2, 3, 9], None));
        let mut ops: Vec<Box<dyn PushOperator>> = vec![
            Box::new(FilterPush {
                col: 0,
                pred: Pred::I64Range { lo: 0, hi: 3 },
            }),
            Box::new(ProbePush::narrow(0, ps)),
        ];
        let mut sink = CollectI64 {
            col: 1,
            out: vec![],
        }; // emit col1 (payload-ish)
        run_pipeline(&mut ops, [m], &mut sink).unwrap();
        // rows surviving filter: keys 1,2,3 → in-set {2,3} → col1 = 20,30
        assert_eq!(sink.out, vec![20, 30]);
    }

    #[test]
    fn empty_selection_propagates() {
        let m = morsel_i64(vec![vec![1, 2, 3]]);
        let ps = Arc::new(choose(&[99], None));
        let mut ops: Vec<Box<dyn PushOperator>> = vec![Box::new(ProbePush::narrow(0, ps))];
        let mut sink = CollectI64 {
            col: 0,
            out: vec![],
        };
        run_pipeline(&mut ops, [m], &mut sink).unwrap();
        assert!(sink.out.is_empty());
    }
}
