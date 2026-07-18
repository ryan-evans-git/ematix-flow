//! P2.2 driver gate: the native-scan, row-group-parallel pipeline driver
//! must decode every row and merge per-thread sinks correctly.

use std::path::PathBuf;

use ematix_flow_engine::chunk::DataChunk;
use ematix_flow_engine::exec::{PushOp, Sink, run_scan_pipeline};
use ematix_flow_engine::scan_native::NativeColKind;

fn sf1_lineitem() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../examples/tpch/data/sf1/lineitem.parquet")
}

/// Counts live rows across every chunk it consumes.
#[derive(Default)]
struct CountSink {
    n: u64,
}
impl Sink for CountSink {
    fn consume(&mut self, chunk: &DataChunk) {
        self.n += chunk.sel.len() as u64;
    }
}

#[test]
fn driver_decodes_all_rows_across_threads() {
    let path = sf1_lineitem();
    if !path.exists() {
        eprintln!(
            "SKIP driver_decodes_all_rows_across_threads: {} absent",
            path.display()
        );
        return;
    }
    // No ops: count every decoded row. One column (l_orderkey=0) suffices.
    let ops: Vec<Box<dyn PushOp>> = vec![];
    let cols = &[(0usize, NativeColKind::I64)];
    let sinks =
        run_scan_pipeline(&path, cols, &ops, CountSink::default, 4).expect("driver failed");
    let total: u64 = sinks.iter().map(|s| s.n).sum();
    assert_eq!(
        total, 6_001_215,
        "driver must decode every lineitem row exactly once across threads"
    );
}
