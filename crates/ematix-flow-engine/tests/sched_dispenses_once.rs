//! P2.3 scheduler gate: the shared morsel dispenser must hand out every
//! morsel index exactly once across any number of concurrent workers —
//! no duplicates (double-decode), no drops (lost rows). This is the
//! correctness invariant that lets the driver swap static striding for a
//! work-sharing queue without touching the operators or the sink.

use std::sync::Arc;

use ematix_flow_engine::sched::MorselQueue;

#[test]
fn dispenses_every_morsel_exactly_once() {
    const N: usize = 10_000;
    const THREADS: usize = 8;

    let q = Arc::new(MorselQueue::new(N));

    let claimed: Vec<Vec<usize>> = std::thread::scope(|scope| {
        let handles: Vec<_> = (0..THREADS)
            .map(|_| {
                let q = Arc::clone(&q);
                scope.spawn(move || {
                    let mut mine = Vec::new();
                    while let Some(i) = q.next() {
                        mine.push(i);
                    }
                    mine
                })
            })
            .collect();
        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    // Every index in 0..N appears exactly once across all workers.
    let mut seen = vec![0u32; N];
    let mut total = 0usize;
    for per_thread in &claimed {
        for &i in per_thread {
            assert!(i < N, "dispenser handed out out-of-range morsel {i}");
            seen[i] += 1;
            total += 1;
        }
    }
    assert_eq!(
        total, N,
        "dispenser handed out {total} morsels, expected {N}"
    );
    assert!(
        seen.iter().all(|&c| c == 1),
        "every morsel must be dispensed exactly once (found a duplicate or a drop)"
    );

    // A drained queue keeps returning None (workers exit cleanly).
    assert_eq!(q.next(), None, "drained queue must stay drained");
    assert_eq!(q.next(), None, "drained queue must stay drained");
}

/// An empty queue hands out nothing (zero-row-group file / fully pruned scan).
#[test]
fn empty_queue_dispenses_nothing() {
    let q = MorselQueue::new(0);
    assert_eq!(q.total(), 0);
    assert_eq!(q.next(), None);
}
