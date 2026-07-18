//! The engine's push execution framework: stateless pipelined operators,
//! stateful terminal sinks (pipeline breakers), and a row-group-parallel
//! driver over the native scan.
//!
//! This generalizes P1's hand-written Q08 loop into reusable pieces. P1
//! (and P2.2) assigned row groups to workers by a static stride; **P2.3
//! replaces that with a shared morsel dispenser** ([`crate::sched`]) so
//! the work balances itself under row-group skew and stragglers — the
//! operators, sinks, and decode path are untouched, only the scheduling
//! changed.

use std::path::Path;
use std::sync::Arc;

use ematix_parquet_io::ParquetFile;

use crate::chunk::DataChunk;
use crate::join::{ProbeStructure, probe_narrow};
use crate::scan_native::{NativeColKind, decode_row_group};
use crate::sched::MorselQueue;

/// A stateless pipelined push operator: narrows a chunk's selection (or
/// attaches a column) for the next stage. Shared read-only across worker
/// threads. Selection-narrowing / attach only — fan-out via emit (for
/// multi-match joins) is a later extension.
pub trait PushOp: Send + Sync {
    fn apply(&self, chunk: DataChunk) -> DataChunk;
}

/// A terminal, stateful consumer — a pipeline breaker's absorb side. One
/// instance per worker thread; the caller merges the per-thread sinks
/// after the driver returns.
pub trait Sink {
    fn consume(&mut self, chunk: &DataChunk);
}

/// Semi-join membership narrow (the no-materialization hot op): keep only
/// the rows whose key (i64 column `key_col`) is a member of `probe`.
pub struct ProbeNarrowOp {
    pub key_col: usize,
    pub probe: Arc<ProbeStructure>,
}

impl PushOp for ProbeNarrowOp {
    fn apply(&self, mut chunk: DataChunk) -> DataChunk {
        chunk.sel = probe_narrow(&chunk, self.key_col, &self.probe);
        chunk
    }
}

/// Native-scan, row-group-parallel pipeline driver. Each worker decodes
/// its row groups via the engine's native scan, pushes each chunk through
/// `ops`, and feeds a thread-local `Sink` built by `make_sink`. Returns
/// the per-thread sinks for the caller to merge.
///
/// `columns` are the leaf indices to decode (chunk column order); `ops`
/// run in order per chunk; the sink absorbs the final chunk. Row groups
/// are the morsels: workers pull the next one from a shared
/// [`MorselQueue`] rather than owning a static stride, so an early
/// finisher absorbs the next available row group and skew/stragglers stop
/// setting the wall.
pub fn run_scan_pipeline<S, F>(
    path: &Path,
    columns: &[(usize, NativeColKind)],
    ops: &[Box<dyn PushOp>],
    make_sink: F,
    nthreads: usize,
) -> Result<Vec<S>, String>
where
    S: Sink + Send,
    F: Fn() -> S + Sync,
{
    let file = ParquetFile::open(path).map_err(|e| format!("open {}: {e}", path.display()))?;
    let md = file.metadata().map_err(|e| format!("metadata: {e}"))?;
    let n_rg = md.row_groups.len();
    let nrows_of: Vec<usize> = (0..n_rg)
        .map(|rg| md.row_groups[rg].num_rows as usize)
        .collect();
    drop(md); // release the metadata borrow; the workers use nrows_of.

    // One morsel per row group, dispensed on demand (P2.3). Never spawn
    // more workers than there are morsels — the surplus would only race to
    // an empty queue.
    let queue = MorselQueue::new(n_rg);
    let nworkers = nthreads.min(n_rg.max(1));

    let file_ref = &file;
    let nrows_ref = &nrows_of;
    let sink_ref = &make_sink;
    let queue_ref = &queue;

    std::thread::scope(|scope| -> Result<Vec<S>, String> {
        let handles: Vec<_> = (0..nworkers)
            .map(|_| {
                scope.spawn(move || -> Result<S, String> {
                    let mut sink = sink_ref();
                    while let Some(rg) = queue_ref.next() {
                        let mut chunk = decode_row_group(file_ref, rg, nrows_ref[rg], columns)?;
                        for op in ops {
                            chunk = op.apply(chunk);
                        }
                        sink.consume(&chunk);
                    }
                    Ok(sink)
                })
            })
            .collect();

        let mut sinks = Vec::with_capacity(nworkers);
        for h in handles {
            sinks.push(
                h.join()
                    .map_err(|_| "worker thread panicked".to_string())??,
            );
        }
        Ok(sinks)
    })
}
