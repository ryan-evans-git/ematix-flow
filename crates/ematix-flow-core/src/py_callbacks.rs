//! Process-global registry of callbacks the Rust runtime can invoke
//! back into the embedding language (typically Python via PyO3).
//!
//! ## Why
//!
//! Several backends — most pressingly the Kafka backend's Glue Schema
//! Registry decode path (task #556), and the upcoming Rust executor
//! for `@ematix.warehouse_pipeline` (task #559) — need to call into
//! Python code from Rust without taking a hard PyO3 dependency in
//! `ematix-flow-core`. The Python SDKs (boto3, the Glue Schema
//! Registry client, snowflake-connector-python) are the canonical
//! surfaces; reimplementing them in Rust would multiply maintenance.
//!
//! ## Shape
//!
//! Callbacks are keyed by string name. The argument and return values
//! are opaque byte slices — the caller and callee agree on a JSON
//! encoding (or any other format) per-callback. This keeps the trait
//! object size-stable and decouples `ematix-flow-core` from any
//! Python-specific types.
//!
//! Concrete Python-binding code lives in `ematix-flow-py` and
//! converts the byte-slice payloads to / from Python objects.
//!
//! ## Concurrency
//!
//! The registry is a global `RwLock<HashMap>`. Lookup is cheap
//! (read-lock), registration is rare (write-lock at startup).
//! Callback invocation drops the lock before calling the closure
//! so a long-running callback doesn't block other lookups.
//!
//! ## Failure model
//!
//! [`CallbackRegistry::invoke`] returns a `CallbackError` when:
//!
//! - the named callback isn't registered (typically: bootstrap order
//!   wrong — Python hasn't imported the module that registers it),
//! - the callback itself returned an error (the byte-slice payload
//!   carries the upstream message verbatim).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

/// A registered callback: takes opaque bytes, returns opaque bytes or
/// an error message. Implementations typically:
///
/// 1. Decode the bytes into the argument type the callee expects
///    (JSON is the convention).
/// 2. Call into the embedding-language code (Python via PyO3).
/// 3. Encode the result back into bytes.
///
/// The closure is `Send + Sync` so a single registry can be used from
/// any Rayon / Tokio thread the backend runs on.
pub type CallbackFn = Arc<dyn Fn(&[u8]) -> Result<Vec<u8>, String> + Send + Sync>;

/// Errors surfaced by [`CallbackRegistry::invoke`].
#[derive(Debug, thiserror::Error)]
pub enum CallbackError {
    #[error("no callback registered under name {0:?}")]
    NotRegistered(String),
    #[error("callback {name:?} returned an error: {message}")]
    CallbackFailed { name: String, message: String },
}

/// Process-global registry of callbacks. Use [`global()`] to access
/// the singleton; the type itself is also constructable for unit
/// tests that want an isolated registry.
#[derive(Default)]
pub struct CallbackRegistry {
    inner: RwLock<HashMap<String, CallbackFn>>,
}

impl CallbackRegistry {
    /// Construct an empty registry. The expected usage is to call
    /// [`global()`] instead; this constructor exists so tests can run
    /// against an isolated registry without polluting the global one.
    pub fn new() -> Self {
        Self::default()
    }

    /// Register `callback` under `name`. If a callback with the same
    /// name is already registered, it is replaced — the most recent
    /// registration wins. This lets tests swap out the production
    /// callback for a stub without restarting the process.
    pub fn register(&self, name: impl Into<String>, callback: CallbackFn) {
        let mut guard = self
            .inner
            .write()
            .expect("CallbackRegistry write lock poisoned");
        guard.insert(name.into(), callback);
    }

    /// Remove a callback. Returns `true` if the callback was
    /// registered (and is now gone), `false` otherwise.
    pub fn unregister(&self, name: &str) -> bool {
        let mut guard = self
            .inner
            .write()
            .expect("CallbackRegistry write lock poisoned");
        guard.remove(name).is_some()
    }

    /// Look up a callback by name without invoking it. Useful when
    /// the caller wants to fail early on a missing registration
    /// before doing work.
    pub fn get(&self, name: &str) -> Option<CallbackFn> {
        let guard = self
            .inner
            .read()
            .expect("CallbackRegistry read lock poisoned");
        guard.get(name).cloned()
    }

    /// Whether a callback is registered under `name`.
    pub fn is_registered(&self, name: &str) -> bool {
        self.get(name).is_some()
    }

    /// Invoke the named callback with `args` and return its bytes.
    ///
    /// Errors are surfaced as [`CallbackError`]: `NotRegistered` if no
    /// callback has been registered yet (typical cause: Python module
    /// hasn't been imported), `CallbackFailed` if the callback raised.
    ///
    /// The registry's read lock is dropped before the callback is
    /// invoked, so a slow callback doesn't block other lookups or
    /// registrations.
    pub fn invoke(&self, name: &str, args: &[u8]) -> Result<Vec<u8>, CallbackError> {
        let cb = self
            .get(name)
            .ok_or_else(|| CallbackError::NotRegistered(name.to_string()))?;
        cb(args).map_err(|message| CallbackError::CallbackFailed {
            name: name.to_string(),
            message,
        })
    }
}

/// Access the process-global callback registry.
///
/// Lazily initialised on first call. Use this from production code;
/// tests that want isolation should construct their own
/// [`CallbackRegistry`] via [`CallbackRegistry::new`].
pub fn global() -> &'static CallbackRegistry {
    static REGISTRY: OnceLock<CallbackRegistry> = OnceLock::new();
    REGISTRY.get_or_init(CallbackRegistry::new)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn echo_cb() -> CallbackFn {
        Arc::new(|args: &[u8]| Ok(args.to_vec()))
    }

    fn fail_cb() -> CallbackFn {
        Arc::new(|_args: &[u8]| Err("synthetic failure".to_string()))
    }

    #[test]
    fn register_and_invoke_round_trips_bytes() {
        let r = CallbackRegistry::new();
        r.register("echo", echo_cb());
        let out = r.invoke("echo", b"hello").unwrap();
        assert_eq!(out, b"hello");
    }

    #[test]
    fn invoke_missing_callback_returns_not_registered() {
        let r = CallbackRegistry::new();
        match r.invoke("nope", b"") {
            Err(CallbackError::NotRegistered(name)) => assert_eq!(name, "nope"),
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn callback_error_propagates_message() {
        let r = CallbackRegistry::new();
        r.register("bad", fail_cb());
        match r.invoke("bad", b"") {
            Err(CallbackError::CallbackFailed { name, message }) => {
                assert_eq!(name, "bad");
                assert!(message.contains("synthetic failure"));
            }
            other => panic!("unexpected: {other:?}"),
        }
    }

    #[test]
    fn register_replaces_previous_binding() {
        let r = CallbackRegistry::new();
        r.register("echo", echo_cb());
        r.register("echo", Arc::new(|_| Ok(b"override".to_vec())));
        let out = r.invoke("echo", b"hello").unwrap();
        assert_eq!(out, b"override");
    }

    #[test]
    fn unregister_returns_true_when_present_false_otherwise() {
        let r = CallbackRegistry::new();
        r.register("echo", echo_cb());
        assert!(r.unregister("echo"));
        assert!(!r.unregister("echo"));
        // After unregister, invoke returns NotRegistered.
        assert!(matches!(
            r.invoke("echo", b""),
            Err(CallbackError::NotRegistered(_))
        ));
    }

    #[test]
    fn is_registered_reports_state() {
        let r = CallbackRegistry::new();
        assert!(!r.is_registered("echo"));
        r.register("echo", echo_cb());
        assert!(r.is_registered("echo"));
    }

    #[test]
    fn concurrent_invoke_does_not_deadlock() {
        // Production case: the callback is held under read-lock but
        // the lock is dropped before invocation, so a slow callback
        // doesn't block another thread from registering / invoking
        // a different callback.
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::thread;

        let r = Arc::new(CallbackRegistry::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let c = counter.clone();
        r.register(
            "slow",
            Arc::new(move |_| {
                std::thread::sleep(std::time::Duration::from_millis(50));
                c.fetch_add(1, Ordering::SeqCst);
                Ok(Vec::new())
            }),
        );
        let r1 = r.clone();
        let r2 = r.clone();
        let h1 = thread::spawn(move || r1.invoke("slow", b"").unwrap());
        let h2 = thread::spawn(move || r2.invoke("slow", b"").unwrap());
        h1.join().unwrap();
        h2.join().unwrap();
        assert_eq!(counter.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn global_registry_is_a_singleton() {
        // Two calls to global() return the same instance (referential
        // identity is the test). Using a name unique to this test so
        // it doesn't collide with other test modules that touch the
        // global.
        global().register(
            "py_callbacks_test::singleton",
            Arc::new(|_| Ok(b"ok".to_vec())),
        );
        assert!(global().is_registered("py_callbacks_test::singleton"));
        // Cleanup so other tests don't see this binding.
        global().unregister("py_callbacks_test::singleton");
    }
}
