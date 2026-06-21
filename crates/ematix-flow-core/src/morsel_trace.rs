//! Morsel-engine P1 de-risk: per-row-group decode trace.
//!
//! Env-gated (`EMAT_MORSEL_TRACE=1`) instrumentation that records one
//! event per `load_row_group` call: the decode worker thread, the RG
//! index, its row/column counts, and start/end timestamps relative to a
//! resettable epoch. From these events we reconstruct a **per-core
//! busy/idle timeline** of the parquet decode to localize the
//! parallelism gap — load imbalance (reclaimable by finer morsels) vs
//! per-RG overhead vs channel backpressure (a downstream problem, not a
//! decode one) — *before* committing to the morsel-region build.
//!
//! Cost when disabled: one cached `bool` read per RG (≈60 RG/query).
//! Cost when enabled: two `Instant::now()` + one `Mutex<Vec>` push per
//! RG. Per-RG granularity keeps the global mutex uncontended (~60
//! pushes/query); a finer (per-column/page) trace would need per-thread
//! buffers instead. See `docs/plans/MORSEL_ENGINE.md`.

use std::sync::{Mutex, OnceLock};
use std::time::Instant;

/// One decode work-unit: a single `load_row_group` call.
#[derive(Clone, Copy)]
pub struct DecodeEvent {
    /// Stable per-thread id (hash of `ThreadId`); the analyzer relabels
    /// to dense 0..K in first-start order.
    pub thread_id: u64,
    pub rg: u32,
    /// Rows in the RG (the decode work size, even on the masked path
    /// where fewer rows survive the filter).
    pub n_rows: u32,
    /// Projected leaf-column count for this scan.
    pub n_cols: u16,
    /// Nanoseconds from the trace epoch to decode start / end.
    pub start_ns: u64,
    pub end_ns: u64,
}

struct TraceState {
    epoch: Instant,
    events: Vec<DecodeEvent>,
}

static ENABLED: OnceLock<bool> = OnceLock::new();
static STATE: OnceLock<Mutex<TraceState>> = OnceLock::new();

/// Whether `EMAT_MORSEL_TRACE` is set. Cached after first read.
#[inline]
pub fn enabled() -> bool {
    *ENABLED.get_or_init(|| {
        std::env::var("EMAT_MORSEL_TRACE")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false)
    })
}

fn state() -> &'static Mutex<TraceState> {
    STATE.get_or_init(|| {
        Mutex::new(TraceState {
            epoch: Instant::now(),
            events: Vec::with_capacity(8192),
        })
    })
}

fn thread_id_u64() -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    std::thread::current().id().hash(&mut h);
    h.finish()
}

/// Begin a decode span. Returns `Some(Instant)` when tracing is on (the
/// caller threads it back into [`end_span`]); `None` is a no-op marker.
#[inline]
pub fn start_span() -> Option<Instant> {
    if enabled() {
        Some(Instant::now())
    } else {
        None
    }
}

/// Close a span opened by [`start_span`] and record the event. A `None`
/// start (tracing disabled) is a no-op.
#[inline]
pub fn end_span(start: Option<Instant>, rg: usize, n_rows: usize, n_cols: usize) {
    let Some(start) = start else {
        return;
    };
    let end = Instant::now();
    let tid = thread_id_u64();
    let st = state();
    let mut g = st.lock().unwrap();
    let epoch = g.epoch;
    let start_ns = start.saturating_duration_since(epoch).as_nanos() as u64;
    let end_ns = end.saturating_duration_since(epoch).as_nanos() as u64;
    g.events.push(DecodeEvent {
        thread_id: tid,
        rg: rg as u32,
        n_rows: n_rows as u32,
        n_cols: n_cols as u16,
        start_ns,
        end_ns,
    });
}

/// Clear recorded events and reset the epoch to now. Call right before
/// the single execution you want the dumped timeline to reflect (after
/// warmups), so all timestamps are relative to that run's start.
pub fn reset() {
    if !enabled() {
        return;
    }
    let st = state();
    let mut g = st.lock().unwrap();
    g.events.clear();
    g.epoch = Instant::now();
}

/// Number of events currently buffered.
pub fn len() -> usize {
    if !enabled() {
        return 0;
    }
    state().lock().unwrap().events.len()
}

/// Write the buffered events to `path` as CSV
/// (`thread_id,rg,n_rows,n_cols,start_ns,end_ns`). Returns the row count.
pub fn dump(path: &str) -> std::io::Result<usize> {
    let st = state();
    let g = st.lock().unwrap();
    let mut s = String::with_capacity(64 + g.events.len() * 48);
    s.push_str("thread_id,rg,n_rows,n_cols,start_ns,end_ns\n");
    for e in &g.events {
        s.push_str(&format!(
            "{},{},{},{},{},{}\n",
            e.thread_id, e.rg, e.n_rows, e.n_cols, e.start_ns, e.end_ns
        ));
    }
    std::fs::write(path, s)?;
    Ok(g.events.len())
}
