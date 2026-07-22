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
    Utf8,
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
    /// UTF-8 strings, Arrow-style: row `i` is `data[offsets[i]..offsets[i+1]]`;
    /// `offsets` has `nrows + 1` entries. For dimension-table decode (the
    /// string predicates in Q08's probe builds); the hot numeric scans stay
    /// flat. One buffer, no per-string allocation.
    Utf8 {
        offsets: Arc<Vec<u32>>,
        data: Arc<Vec<u8>>,
    },
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

    /// Wrap a UTF-8 string column: `offsets` (`nrows + 1` entries) into a
    /// single `data` byte buffer. Row `i` is `data[offsets[i]..offsets[i+1]]`.
    pub fn utf8(offsets: Vec<u32>, data: Vec<u8>) -> Self {
        debug_assert!(!offsets.is_empty(), "utf8 offsets need nrows+1 entries");
        // Validate ONCE here — whole buffer + every offset on a char
        // boundary — so `Utf8View::get` can slice UNCHECKED. Per-access
        // `from_utf8` was a top-3 cost in string-residual join loops
        // (q59: 17M fanned rows × get × validate). A valid buffer sliced
        // at char boundaries yields valid UTF-8 at every offset pair.
        std::str::from_utf8(&data).expect("BYTE_ARRAY not valid UTF-8");
        for &o in &offsets {
            let o = o as usize;
            assert!(
                o <= data.len() && (o == data.len() || data[o] & 0xC0 != 0x80),
                "utf8 offset {o} not on a char boundary"
            );
        }
        Vector {
            logical: LogicalType::Utf8,
            storage: Storage::Utf8 {
                offsets: Arc::new(offsets),
                data: Arc::new(data),
            },
            validity: None,
        }
    }

    /// Attach (or clear) a validity mask — `false` marks a NULL row. The
    /// decode paths call this with the mask expanded from parquet
    /// definition levels; all-valid columns pass `None` so downstream
    /// hot loops stay branch-free.
    pub fn with_validity(mut self, validity: Option<Vec<bool>>) -> Self {
        self.validity = validity.map(Arc::from);
        self
    }

    pub fn len(&self) -> usize {
        match &self.storage {
            Storage::I32(v) => v.len(),
            Storage::I64(v) => v.len(),
            Storage::F64(v) => v.len(),
            Storage::Utf8 { offsets, .. } => offsets.len().saturating_sub(1),
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

    /// Borrow the string column as a [`Utf8View`]. Panics if storage is not
    /// Utf8. The view hoists the storage match out of a per-row loop, then
    /// `get(i)` returns row `i` as `&str`.
    #[inline]
    pub fn as_utf8(&self) -> Utf8View<'_> {
        match &self.storage {
            Storage::Utf8 { offsets, data } => Utf8View { offsets, data },
            _ => panic!("as_utf8 on non-Utf8 vector"),
        }
    }
}

/// A borrowed view over a [`Storage::Utf8`] column: offsets + one byte
/// buffer, with `get(i) -> &str`.
pub struct Utf8View<'a> {
    offsets: &'a [u32],
    data: &'a [u8],
}

impl<'a> Utf8View<'a> {
    /// Row `i` as a string slice. Panics on a non-UTF-8 byte range (a
    /// decode/wiring bug — TPC-H string columns are ASCII).
    #[inline]
    pub fn get(&self, i: usize) -> &'a str {
        let s = self.offsets[i] as usize;
        let e = self.offsets[i + 1] as usize;
        // SAFETY: `Vector::utf8` (the only `Storage::Utf8` constructor)
        // validated the whole buffer and every offset's char boundary, so
        // any offset-pair slice is valid UTF-8.
        debug_assert!(std::str::from_utf8(&self.data[s..e]).is_ok());
        unsafe { std::str::from_utf8_unchecked(&self.data[s..e]) }
    }

    pub fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The unchecked `Utf8View::get` is sound only because construction
    /// validates — invalid bytes must panic HERE, not slip through.
    #[test]
    #[should_panic(expected = "not valid UTF-8")]
    fn utf8_constructor_rejects_invalid_bytes() {
        Vector::utf8(vec![0, 2], vec![0xFF, 0xFE]);
    }

    /// An offset landing inside a multi-byte char must panic too — a
    /// valid buffer sliced off a char boundary is not valid UTF-8.
    #[test]
    #[should_panic(expected = "char boundary")]
    fn utf8_constructor_rejects_split_char() {
        // 'é' = 0xC3 0xA9; offset 1 splits it.
        Vector::utf8(vec![0, 1, 2], "é".as_bytes().to_vec());
    }

    #[test]
    fn utf8_get_roundtrip() {
        let v = Vector::utf8(vec![0, 5, 11], b"helloworld!".to_vec());
        let view = v.as_utf8();
        assert_eq!(view.get(0), "hello");
        assert_eq!(view.get(1), "world!");
    }
}
