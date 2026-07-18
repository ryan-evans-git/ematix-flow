//! The engine's push execution framework: stateless pipelined operators,
//! stateful terminal sinks (pipeline breakers), and a row-group-parallel
//! driver over the native scan.
//!
//! This generalizes P1's hand-written Q08 loop into reusable pieces. The
//! driver still assigns row groups to workers by static stride (as P1
//! did); **P2.3 replaces that with a work-stealing morsel queue** — the
//! operators, sinks, and decode path stay put, only the scheduling
//! changes.

use std::path::Path;
use std::sync::Arc;

use ematix_parquet_io::ParquetFile;

use crate::chunk::DataChunk;
use crate::join::{ProbeStructure, probe_narrow};
use crate::scan_native::{NativeColKind, decode_row_group};

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
/// are assigned to workers by static stride `rg % nthreads` — the piece
/// P2.3 swaps for work-stealing.
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

    let file_ref = &file;
    let nrows_ref = &nrows_of;
    let sink_ref = &make_sink;

    std::thread::scope(|scope| -> Result<Vec<S>, String> {
        let handles: Vec<_> = (0..nthreads)
            .map(|t| {
                scope.spawn(move || -> Result<S, String> {
                    let mut sink = sink_ref();
                    let mut rg = t;
                    while rg < n_rg {
                        let mut chunk = decode_row_group(file_ref, rg, nrows_ref[rg], columns)?;
                        for op in ops {
                            chunk = op.apply(chunk);
                        }
                        sink.consume(&chunk);
                        rg += nthreads;
                    }
                    Ok(sink)
                })
            })
            .collect();

        let mut sinks = Vec::with_capacity(nthreads);
        for h in handles {
            sinks.push(h.join().map_err(|_| "worker thread panicked".to_string())??);
        }
        Ok(sinks)
    })
}
