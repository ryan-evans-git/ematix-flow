//! Σ.D3: cranelift-JIT'd inner loop for the unified `FusedAggregateExec`.
//!
//! Σ.D1 and Σ.D2 each ship a hard-coded operator that hits ≤ 3.06 ms inside
//! DataFusion's runtime on its target query. To generalize that win across
//! arbitrary `Aggregate(SUM/COUNT/AVG) over Filter(AND-chain)` plans without
//! the 3–5× slowdown of per-clause `match` dispatch in a generic interpreter
//! loop, this module emits **machine code per plan** at construction time
//! via cranelift-jit.
//!
//! The strategy: at plan-time the operator translates its `FusedPredicate`
//! and aggregate spec into cranelift IR — comparisons unrolled inline, no
//! BooleanArray materialization, no per-row dispatch — then JITs to a
//! function pointer. The execute-time hot loop is exactly the same shape
//! as the hand-written Σ.D1 / Σ.D2 inner loops, just generated for the
//! concrete plan.
//!
//! This file is the day-1 scaffold for that. It builds the smallest
//! useful JIT — the **Q6 predicate evaluator** — as a proof of the
//! integration path. It compiles a function of signature
//!
//! ```text
//! fn q6_predicate_eval(
//!     n: i64,                  // row count
//!     shipdate: *const i32,    // Date32 raw values
//!     discount: *const f64,    // Float64 raw values
//!     quantity: *const f64,    // Float64 raw values
//!     extprice: *const f64,    // Float64 raw values, for SUM input
//!     out_sum: *mut f64,       // running sum (caller-initialized to 0)
//! );
//! ```
//!
//! which walks the rows, evaluates the Q6 AND-chain predicate inline, and
//! accumulates `extprice * discount` into `*out_sum` for each matching
//! row. **No materialized mask.** Same algorithm as Σ.D1's hand-written
//! `run_fused_shard`, but with the predicate constants emitted as
//! immediates in the JIT'd code instead of read from a struct.
//!
//! Day-2 scope (separate commit): build a `FusedPredicate` /
//! `FusedAggregate` data model + a generic IR emitter that translates
//! arbitrary plan shapes (not just Q6) into JIT'd code. The constants
//! become arguments / loaded from a config block at runtime. Then the
//! planner rule (phase 4 of issue #45) routes matching plans to this
//! path.

use std::mem;

use cranelift::prelude::*;
use cranelift_codegen::ir::{
    AbiParam, Function, InstBuilder, Signature, StackSlot, StackSlotData, StackSlotKind,
    UserFuncName,
};
use cranelift_codegen::isa::CallConv;
use cranelift_codegen::settings;
use cranelift_codegen::verifier::verify_function;
use cranelift_frontend::{FunctionBuilder, FunctionBuilderContext};
use cranelift_jit::{JITBuilder, JITModule};
use cranelift_module::{Linkage, Module};

/// JIT'd Q6 predicate evaluator. Owned by the caller for the lifetime of
/// any `func_ptr` it holds — dropping the wrapper frees the underlying
/// machine code.
pub struct Q6JitFn {
    _module: JITModule,
    func_ptr: *const u8,
}

/// `extern "C"` signature of the JIT'd function, matched by the cranelift
/// emitter below.
pub type Q6PredicateFn = unsafe extern "C" fn(
    n: i64,
    shipdate: *const i32,
    discount: *const f64,
    quantity: *const f64,
    extprice: *const f64,
    out_sum: *mut f64,
);

impl Q6JitFn {
    /// Build, verify, and JIT-compile the Q6 predicate evaluator for the
    /// canonical TPC-H bounds:
    ///   * `shipdate ∈ [1994-01-01=8766, 1995-01-01=9131)`
    ///   * `discount ∈ [0.05, 0.07]`
    ///   * `quantity < 24.0`
    ///
    /// And the canonical SUM input `extprice * discount`.
    ///
    /// Returns a wrapper holding the JIT module + function pointer.
    pub fn try_build_q6_canonical() -> Result<Self, String> {
        Self::try_build(8766, 9131, 0.05, 0.07, 24.0)
    }

    /// Build the JIT for a parametrized version of the Q6 predicate
    /// shape. Useful for testing on the small batches the unit test
    /// constructs (any Date32 / discount bounds, not just SF=1's
    /// values).
    pub fn try_build(
        date_lo: i32,
        date_hi: i32,
        disc_lo: f64,
        disc_hi: f64,
        qty_hi: f64,
    ) -> Result<Self, String> {
        // ----- 1. JIT module + ISA -----
        let mut flag_builder = settings::builder();
        flag_builder
            .set("use_colocated_libcalls", "false")
            .map_err(|e| format!("flag use_colocated_libcalls: {e}"))?;
        flag_builder
            .set("is_pic", "false")
            .map_err(|e| format!("flag is_pic: {e}"))?;
        // Opt level: speed. Speed_and_size is also fine but slower to
        // compile; we want minimal plan-time overhead.
        flag_builder
            .set("opt_level", "speed")
            .map_err(|e| format!("flag opt_level: {e}"))?;
        let flags = settings::Flags::new(flag_builder);
        let isa_builder =
            cranelift_native::builder().map_err(|e| format!("cranelift_native::builder: {e}"))?;
        let isa = isa_builder
            .finish(flags)
            .map_err(|e| format!("isa.finish: {e}"))?;

        let builder = JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());
        let mut module = JITModule::new(builder);

        // ----- 2. Signature: 5 pointers + i64 + return void -----
        let ptr_ty = module.target_config().pointer_type();
        let mut sig = Signature::new(CallConv::SystemV);
        sig.params.push(AbiParam::new(types::I64)); // n
        sig.params.push(AbiParam::new(ptr_ty)); // shipdate *const i32
        sig.params.push(AbiParam::new(ptr_ty)); // discount *const f64
        sig.params.push(AbiParam::new(ptr_ty)); // quantity *const f64
        sig.params.push(AbiParam::new(ptr_ty)); // extprice *const f64
        sig.params.push(AbiParam::new(ptr_ty)); // out_sum  *mut f64

        let func_id = module
            .declare_function("q6_predicate_eval", Linkage::Local, &sig)
            .map_err(|e| format!("declare_function: {e}"))?;

        // ----- 3. Build the IR -----
        let mut ctx = module.make_context();
        ctx.func = Function::with_name_signature(UserFuncName::default(), sig.clone());
        let mut fb_ctx = FunctionBuilderContext::new();
        let mut builder = FunctionBuilder::new(&mut ctx.func, &mut fb_ctx);

        let entry = builder.create_block();
        let loop_header = builder.create_block();
        let loop_body = builder.create_block();
        let row_match = builder.create_block();
        let row_skip = builder.create_block();
        let loop_exit = builder.create_block();

        // Block params for entry: receive function arguments
        builder.append_block_params_for_function_params(entry);

        // Loop header carries: i (i64), running sum (f64)
        builder.append_block_param(loop_header, types::I64);
        builder.append_block_param(loop_header, types::F64);

        builder.switch_to_block(entry);
        builder.seal_block(entry);
        let p_n = builder.block_params(entry)[0];
        let p_shipdate = builder.block_params(entry)[1];
        let p_discount = builder.block_params(entry)[2];
        let p_quantity = builder.block_params(entry)[3];
        let p_extprice = builder.block_params(entry)[4];
        let p_out_sum = builder.block_params(entry)[5];

        // Load initial running sum from *out_sum (so the caller can
        // pre-seed it — e.g. for cross-shard merging) and jump to loop.
        let init_sum = builder
            .ins()
            .load(types::F64, MemFlags::trusted(), p_out_sum, 0);
        let zero_i = builder.ins().iconst(types::I64, 0);
        builder.ins().jump(loop_header, &[zero_i, init_sum]);

        // ----- Loop header: i < n ? body : exit -----
        builder.switch_to_block(loop_header);
        let i = builder.block_params(loop_header)[0];
        let running_sum = builder.block_params(loop_header)[1];
        let cmp = builder.ins().icmp(IntCC::SignedLessThan, i, p_n);
        builder.ins().brif(cmp, loop_body, &[], loop_exit, &[]);

        // ----- Loop body: evaluate predicate inline -----
        builder.switch_to_block(loop_body);
        builder.seal_block(loop_body);

        // ship = shipdate[i] (i32)
        let i32_size = builder.ins().iconst(types::I64, 4);
        let ship_off = builder.ins().imul(i, i32_size);
        let ship_ptr = builder.ins().iadd(p_shipdate, ship_off);
        let ship_v = builder
            .ins()
            .load(types::I32, MemFlags::trusted(), ship_ptr, 0);

        // ship >= date_lo  AND  ship < date_hi
        let lo_imm = builder.ins().iconst(types::I32, date_lo as i64);
        let hi_imm = builder.ins().iconst(types::I32, date_hi as i64);
        let ge_lo = builder
            .ins()
            .icmp(IntCC::SignedGreaterThanOrEqual, ship_v, lo_imm);
        let lt_hi = builder.ins().icmp(IntCC::SignedLessThan, ship_v, hi_imm);
        let pass_date = builder.ins().band(ge_lo, lt_hi);

        // disc = discount[i] (f64)
        let f64_size = builder.ins().iconst(types::I64, 8);
        let f_off = builder.ins().imul(i, f64_size);
        let disc_ptr = builder.ins().iadd(p_discount, f_off);
        let disc_v = builder
            .ins()
            .load(types::F64, MemFlags::trusted(), disc_ptr, 0);

        // disc >= disc_lo  AND  disc <= disc_hi
        let disc_lo_v = builder.ins().f64const(disc_lo);
        let disc_hi_v = builder.ins().f64const(disc_hi);
        let disc_ge = builder
            .ins()
            .fcmp(FloatCC::GreaterThanOrEqual, disc_v, disc_lo_v);
        let disc_le = builder
            .ins()
            .fcmp(FloatCC::LessThanOrEqual, disc_v, disc_hi_v);
        let pass_disc = builder.ins().band(disc_ge, disc_le);

        // qty = quantity[i] (f64); qty < qty_hi
        let qty_ptr = builder.ins().iadd(p_quantity, f_off);
        let qty_v = builder
            .ins()
            .load(types::F64, MemFlags::trusted(), qty_ptr, 0);
        let qty_hi_v = builder.ins().f64const(qty_hi);
        let pass_qty = builder.ins().fcmp(FloatCC::LessThan, qty_v, qty_hi_v);

        // Combined AND: pass_date AND pass_disc AND pass_qty.
        let pass_date_disc = builder.ins().band(pass_date, pass_disc);
        let pass_all = builder.ins().band(pass_date_disc, pass_qty);

        // Branch on pass: match → row_match, otherwise → row_skip.
        builder.ins().brif(pass_all, row_match, &[], row_skip, &[]);

        // ----- Match path: load extprice, accumulate -----
        builder.switch_to_block(row_match);
        builder.seal_block(row_match);
        let ext_ptr = builder.ins().iadd(p_extprice, f_off);
        let ext_v = builder
            .ins()
            .load(types::F64, MemFlags::trusted(), ext_ptr, 0);
        let prod = builder.ins().fmul(ext_v, disc_v);
        let new_sum = builder.ins().fadd(running_sum, prod);
        let next_i = builder.ins().iadd_imm(i, 1);
        builder.ins().jump(loop_header, &[next_i, new_sum]);

        // ----- Skip path: just increment i, keep sum -----
        builder.switch_to_block(row_skip);
        builder.seal_block(row_skip);
        let next_i_skip = builder.ins().iadd_imm(i, 1);
        builder.ins().jump(loop_header, &[next_i_skip, running_sum]);

        // The loop_header has incoming edges from entry, row_match, and
        // row_skip — must be sealed *after* row_match and row_skip.
        builder.seal_block(loop_header);

        // ----- Exit: store the final sum back to *out_sum -----
        builder.switch_to_block(loop_exit);
        builder.seal_block(loop_exit);
        let final_sum = builder.block_params(loop_header)[1];
        builder
            .ins()
            .store(MemFlags::trusted(), final_sum, p_out_sum, 0);
        builder.ins().return_(&[]);

        builder.finalize();

        // ----- 4. Verify + define -----
        verify_function(&ctx.func, module.isa()).map_err(|e| format!("verify_function: {e}"))?;
        module
            .define_function(func_id, &mut ctx)
            .map_err(|e| format!("define_function: {e}"))?;
        module.clear_context(&mut ctx);

        // ----- 5. Finalize, get the function pointer -----
        module
            .finalize_definitions()
            .map_err(|e| format!("finalize_definitions: {e}"))?;
        let func_ptr = module.get_finalized_function(func_id);
        Ok(Self {
            _module: module,
            func_ptr,
        })
    }

    /// Run the JIT'd predicate over the column slices, accumulating the
    /// running sum into the caller's mutable f64. Safety: the slices
    /// must be at least `n` elements long and live for the call.
    ///
    /// # Safety
    ///
    /// Caller must guarantee that:
    /// - `shipdate.len() >= n`, `discount.len() >= n`,
    ///   `quantity.len() >= n`, `extprice.len() >= n`.
    /// - `out_sum` is a valid pointer to f64.
    pub unsafe fn run(
        &self,
        n: i64,
        shipdate: *const i32,
        discount: *const f64,
        quantity: *const f64,
        extprice: *const f64,
        out_sum: *mut f64,
    ) {
        // SAFETY: caller upholds the slice-length + pointer-validity
        // invariants documented above.
        unsafe {
            let func: Q6PredicateFn = mem::transmute(self.func_ptr);
            func(n, shipdate, discount, quantity, extprice, out_sum);
        }
    }
}

// SAFETY: the JIT module holds a leaked CodegenContext but the
// emitted function pointer only reads from the input slices passed
// at call time and writes to an out param. No shared mutable state.
unsafe impl Send for Q6JitFn {}
unsafe impl Sync for Q6JitFn {}

// ===========================================================================
// Σ.D3 phase A: data-driven IR emitter (FusedFilterAggJit).
// ===========================================================================
//
// `Q6JitFn` above is the day-1 scaffold — it bakes Q6's specific predicate
// shape into hard-coded IR builder calls. Phase A factors out the spec from
// the emitter: define a `FusedFilterAggSpec` data model (typed column slots,
// AND-chain predicate, list of aggregates), and emit IR that walks the spec.
// Q6's shape becomes one possible spec; the existing `try_build_q6_canonical`
// reduces to "construct that spec, hand it to the generic builder."
//
// The generic builder is what Σ.D phase B/C/D extend with:
//   * group-by (one accumulator block per group key)
//   * StringView column types (prefix-check on 16-byte view layout)
//   * direct-indexed probe bitmaps (Q14)
//   * CASE-WHEN-guarded aggregates (Q14 PROMO%)
//
// For now we cover the subset needed to retrofit Σ.D1 (Q6 / FusedFilterSumExec):
// Float64 / Date32 / Int32 / Int64 columns, AND-chain predicate, 1+ SUM
// aggregates where each is either `SUM(col)` or `SUM(col_a * col_b)`.

/// Cranelift-renderable column type. Picks the load width + cranelift IR
/// type when the emitter reads `column[i]`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnTy {
    Float64,
    Date32,
    Int32,
    Int64,
    /// Arrow `StringViewArray` — 16-byte view per element. For short
    /// (≤12-byte) inline strings — which is what every TPC-H column
    /// we currently group on contains — the first byte sits at offset
    /// +4 in the view. The IR emitter exploits that for the small-
    /// cardinality "first-byte-of-key" group-by Σ.D2 uses.
    Utf8View,
}

impl ColumnTy {
    /// Byte width of one element in the column buffer.
    fn elem_size(self) -> i64 {
        match self {
            ColumnTy::Float64 | ColumnTy::Int64 => 8,
            ColumnTy::Date32 | ColumnTy::Int32 => 4,
            ColumnTy::Utf8View => 16,
        }
    }
    /// Cranelift IR type for a loaded element. Currently used by the
    /// Phase B/C extensions (StringView, group-by) being built on top
    /// of this scaffold — kept here so the type-mapping lives next to
    /// `elem_size` and stays in sync.
    #[allow(dead_code)]
    fn ir_ty(self) -> Type {
        match self {
            ColumnTy::Float64 => types::F64,
            ColumnTy::Date32 | ColumnTy::Int32 => types::I32,
            ColumnTy::Int64 => types::I64,
            // Utf8View elements are 16-byte views; not a primitive IR
            // type. Callers that need scalar access use the dedicated
            // `emit_load_utf8view_first_byte` helper instead.
            ColumnTy::Utf8View => types::I8,
        }
    }
}

/// One clause of the AND-chain predicate. `column` indexes
/// [`FusedFilterAggSpec::inputs`]. The op picks the comparison, and the
/// immediate (whichever field matches the op's domain) becomes a baked-in
/// constant in the IR.
#[derive(Debug, Clone, Copy)]
pub struct Clause {
    pub column: usize,
    pub op: ClauseOp,
    pub imm_i32: i32,
    pub imm_f64: f64,
}

#[derive(Debug, Clone, Copy)]
pub enum ClauseOp {
    /// Float64 column `>= imm_f64`.
    F64Ge,
    /// Float64 column `<= imm_f64`.
    F64Le,
    /// Float64 column `<  imm_f64`.
    F64Lt,
    /// Float64 column `>  imm_f64`.
    F64Gt,
    /// Int32 (or Date32 = Int32) column `>= imm_i32`.
    I32Ge,
    /// Int32 (or Date32) column `<  imm_i32`.
    I32Lt,
    /// Int32 (or Date32) column `<= imm_i32`.
    I32Le,
    /// Int32 (or Date32) column `>  imm_i32`.
    I32Gt,
}

/// One aggregate expression evaluated for each matching row. The output
/// is always f64 (DataFusion's SUM-of-numeric default — COUNT is also
/// stored as f64 and the host casts back to integer at emit time).
/// One `AggExpr` produces one entry in the JIT'd function's `outputs[]`
/// array per group (or one total for no-group specs).
#[derive(Debug, Clone)]
pub enum AggExpr {
    /// `SUM(col[i])` — column must be Float64.
    SumColumn(usize),
    /// `SUM(col[a] * col[b])` — both columns must be Float64. Q6's
    /// `SUM(l_extendedprice * l_discount)` shape.
    SumProductColumns(usize, usize),
    /// `SUM(col[a] * (1 - col[b]))` — both columns Float64. Q1's
    /// `sum_disc_price = SUM(l_extendedprice * (1 - l_discount))`.
    SumProductOneMinus(usize, usize),
    /// `SUM(col[a] * (1 - col[b]) * (1 + col[c]))` — all Float64. Q1's
    /// `sum_charge = SUM(l_extendedprice * (1 - l_discount) * (1 + l_tax))`.
    SumProductTwoOneMinusOnePlus(usize, usize, usize),
    /// `COUNT(*)` — increments by 1.0 per matching row. Stored as f64;
    /// the host downcasts to i64 at emit time when it needs integer
    /// `count_order` output.
    CountStar,
    /// `SUM(CASE WHEN guard_col STARTS WITH prefix THEN val_col * (1 - disc_col) ELSE 0)`.
    /// Q14's `SUM(CASE WHEN p_type LIKE 'PROMO%' THEN price * (1 - disc) ELSE 0)`.
    ///
    /// `guard_col` must be Utf8View; `val_col`/`disc_col` must be Float64.
    /// `prefix` must be **≤ 4 bytes**: Arrow's StringView guarantees the
    /// first 4 bytes of every string live at view-offset +4 (either as
    /// part of the inline data for ≤12-byte strings, or as the inline
    /// "prefix" portion of the non-inline layout for longer strings).
    /// Bytes 5+ of long strings live in an external buffer that the IR
    /// can't reach without a host callback. For longer prefix checks
    /// the caller must reduce to a 4-byte unique identifier (TPC-H Q14
    /// can use `"PROM"` since no other `p_type` value starts with PROM).
    ///
    /// The IR emits one byte-load per prefix byte, ANDs the equality
    /// checks, and uses `select(match, term, 0.0)` so the loop stays
    /// branchless on the hot path.
    SumProductOneMinusGuardedByPrefix {
        guard_col: usize,
        val_col: usize,
        disc_col: usize,
        prefix: Vec<u8>,
    },
}

/// Optional small-cardinality group-by. When present, `outputs[]` holds
/// `aggregates.len() * (known_keys.len() + 1)` f64 cells laid out
/// row-major: `[g0_agg0, g0_agg1, …, g0_aggM-1, g1_agg0, …, catchall_aggM-1]`.
/// Rows whose key tuple doesn't match any `known_keys` entry go to the
/// catch-all bucket at index `known_keys.len()`.
///
/// Restricted to small-cardinality (e.g. Q1's 4 groups). For larger
/// group-by spaces the IR emitter would need a hash table instead of
/// the IR-chain dispatch we use here.
#[derive(Debug, Clone)]
pub struct GroupSpec {
    /// Indices into `FusedFilterAggSpec::inputs` of the key columns.
    /// Each column must be Utf8View; we read the first byte of each at
    /// row `i` to form the group key (offset +4 in the 16-byte view).
    pub key_columns: Vec<usize>,
    /// Known key tuples. Each entry has length == key_columns.len();
    /// the elements are u8 byte values matched against the first byte
    /// of each key column.
    pub known_keys: Vec<Vec<u8>>,
}

/// Top-level spec. Owns the slots all three families (predicate, aggs,
/// inputs) describe by index. Validated at JIT-build time.
#[derive(Debug, Clone, Default)]
pub struct FusedFilterAggSpec {
    pub inputs: Vec<ColumnTy>,
    pub predicate: Vec<Clause>,
    pub aggregates: Vec<AggExpr>,
    /// Optional group-by. `None` means single-group (Q6 shape) — the
    /// `outputs[]` array has one cell per aggregate. `Some(g)` means
    /// `aggregates.len() * (g.known_keys.len() + 1)` cells.
    pub group: Option<GroupSpec>,
}

impl FusedFilterAggSpec {
    pub fn new() -> Self {
        Self::default()
    }
    /// Q6 spec: shipdate ∈ [date_lo, date_hi), discount ∈ [disc_lo, disc_hi],
    /// quantity < qty_hi; one aggregate, SUM(extprice * discount). Inputs
    /// are ordered as in the hand-coded path: shipdate (0), discount (1),
    /// quantity (2), extprice (3).
    pub fn q6(date_lo: i32, date_hi: i32, disc_lo: f64, disc_hi: f64, qty_hi: f64) -> Self {
        Self {
            inputs: vec![
                ColumnTy::Date32,
                ColumnTy::Float64,
                ColumnTy::Float64,
                ColumnTy::Float64,
            ],
            predicate: vec![
                Clause {
                    column: 0,
                    op: ClauseOp::I32Ge,
                    imm_i32: date_lo,
                    imm_f64: 0.0,
                },
                Clause {
                    column: 0,
                    op: ClauseOp::I32Lt,
                    imm_i32: date_hi,
                    imm_f64: 0.0,
                },
                Clause {
                    column: 1,
                    op: ClauseOp::F64Ge,
                    imm_i32: 0,
                    imm_f64: disc_lo,
                },
                Clause {
                    column: 1,
                    op: ClauseOp::F64Le,
                    imm_i32: 0,
                    imm_f64: disc_hi,
                },
                Clause {
                    column: 2,
                    op: ClauseOp::F64Lt,
                    imm_i32: 0,
                    imm_f64: qty_hi,
                },
            ],
            aggregates: vec![AggExpr::SumProductColumns(3, 1)],
            group: None,
        }
    }

    /// Q1 spec: l_shipdate <= cutoff; grouped by (l_returnflag,
    /// l_linestatus) over the four known TPC-H tuples (R,F)/(N,F)/(N,O)/
    /// (A,F); five SUMs + COUNT per group.
    ///
    /// Input order (matches the hand-coded `FusedFilterMultiAggExec`
    /// validation): returnflag (0), linestatus (1), quantity (2),
    /// extprice (3), discount (4), tax (5), shipdate (6).
    ///
    /// Aggregate order matches `Q1Aggs`'s field order in
    /// `fused_multi_agg.rs`: sum_qty, sum_price, sum_disc_price,
    /// sum_charge, sum_disc, count.
    pub fn q1(shipdate_cutoff: i32) -> Self {
        Self {
            inputs: vec![
                ColumnTy::Utf8View, // 0 l_returnflag
                ColumnTy::Utf8View, // 1 l_linestatus
                ColumnTy::Float64,  // 2 l_quantity
                ColumnTy::Float64,  // 3 l_extendedprice
                ColumnTy::Float64,  // 4 l_discount
                ColumnTy::Float64,  // 5 l_tax
                ColumnTy::Date32,   // 6 l_shipdate
            ],
            predicate: vec![Clause {
                column: 6,
                op: ClauseOp::I32Le,
                imm_i32: shipdate_cutoff,
                imm_f64: 0.0,
            }],
            aggregates: vec![
                AggExpr::SumColumn(2),                          // sum_qty
                AggExpr::SumColumn(3),                          // sum_price
                AggExpr::SumProductOneMinus(3, 4),              // sum_disc_price
                AggExpr::SumProductTwoOneMinusOnePlus(3, 4, 5), // sum_charge
                AggExpr::SumColumn(4),                          // sum_disc
                AggExpr::CountStar,                             // count
            ],
            group: Some(GroupSpec {
                key_columns: vec![0, 1],
                // Order matches `q1_group_idx` in fused_multi_agg.rs so
                // catch-all rows land in the same bucket index (4) in
                // both paths — keeps the bit-identical equivalence test
                // honest.
                known_keys: vec![
                    vec![b'R', b'F'], // 0
                    vec![b'N', b'F'], // 1
                    vec![b'N', b'O'], // 2
                    vec![b'A', b'F'], // 3
                ],
            }),
        }
    }

    /// Q14 post-join spec: matches the `FusedPostJoinExec::Q14` shape.
    /// Inputs are the join output projected to `(p_type, l_extendedprice,
    /// l_discount)`. No predicate (the filter happened before the join
    /// in DataFusion's plan). Two aggregates: the PROMO-guarded SUM and
    /// the unguarded SUM. Output is two f64 cells `[promo, total]`;
    /// the host computes `100 * promo / total` at emit time.
    pub fn q14_post_join() -> Self {
        Self {
            inputs: vec![
                ColumnTy::Utf8View, // 0 p_type
                ColumnTy::Float64,  // 1 l_extendedprice
                ColumnTy::Float64,  // 2 l_discount
            ],
            predicate: vec![],
            aggregates: vec![
                AggExpr::SumProductOneMinusGuardedByPrefix {
                    guard_col: 0,
                    val_col: 1,
                    disc_col: 2,
                    // Q14's spec test is `p_type LIKE 'PROMO%'`. TPC-H
                    // restricts p_type to a fixed enum where no other
                    // value starts with "PROM", so the 4-byte prefix
                    // "PROM" identifies "PROMO..." uniquely. See the
                    // `SumProductOneMinusGuardedByPrefix` docs for why
                    // the JIT is restricted to a 4-byte prefix.
                    prefix: b"PROM".to_vec(),
                },
                AggExpr::SumProductOneMinus(1, 2),
            ],
            group: None,
        }
    }
}

/// Generic JIT-built fused filter+aggregate function. Same lifetime/ABI
/// shape as [`Q6JitFn`], but built from a [`FusedFilterAggSpec`].
pub struct FusedFilterAggJit {
    _module: JITModule,
    func_ptr: *const u8,
    n_inputs: usize,
    n_outputs: usize,
}

/// Fixed-arity FFI for any spec. The IR loads each per-column pointer
/// once at function entry from `inputs[k]`, then runs the loop. Outputs
/// are read pre-seeded (so cross-shard merging can add into a global) and
/// stored back at exit.
pub type FusedFilterAggFn =
    unsafe extern "C" fn(n: i64, inputs: *const *const u8, outputs: *mut f64);

impl FusedFilterAggJit {
    pub fn try_build(spec: &FusedFilterAggSpec) -> Result<Self, String> {
        validate_spec(spec)?;
        let n_inputs = spec.inputs.len();
        let n_aggs = spec.aggregates.len();
        // Output cells: 1 per aggregate for ungrouped, or n_aggs ×
        // (n_known_keys + 1) for grouped (the +1 is the catchall bucket).
        let n_groups = spec
            .group
            .as_ref()
            .map(|g| g.known_keys.len() + 1)
            .unwrap_or(1);
        let n_outputs = n_aggs * n_groups;

        // ----- 1. JIT module + ISA (identical to Q6JitFn) -----
        let mut flag_builder = settings::builder();
        flag_builder
            .set("use_colocated_libcalls", "false")
            .map_err(|e| format!("flag use_colocated_libcalls: {e}"))?;
        flag_builder
            .set("is_pic", "false")
            .map_err(|e| format!("flag is_pic: {e}"))?;
        flag_builder
            .set("opt_level", "speed")
            .map_err(|e| format!("flag opt_level: {e}"))?;
        let flags = settings::Flags::new(flag_builder);
        let isa_builder =
            cranelift_native::builder().map_err(|e| format!("cranelift_native::builder: {e}"))?;
        let isa = isa_builder
            .finish(flags)
            .map_err(|e| format!("isa.finish: {e}"))?;
        let builder = JITBuilder::with_isa(isa, cranelift_module::default_libcall_names());
        let mut module = JITModule::new(builder);
        let ptr_ty = module.target_config().pointer_type();

        // ----- 2. Fixed-arity signature -----
        let mut sig = Signature::new(CallConv::SystemV);
        sig.params.push(AbiParam::new(types::I64)); // n
        sig.params.push(AbiParam::new(ptr_ty)); // inputs: *const *const u8
        sig.params.push(AbiParam::new(ptr_ty)); // outputs: *mut f64

        let func_id = module
            .declare_function("fused_filter_agg", Linkage::Local, &sig)
            .map_err(|e| format!("declare_function: {e}"))?;

        // ----- 3. Build the IR -----
        let mut ctx = module.make_context();
        ctx.func = Function::with_name_signature(UserFuncName::default(), sig.clone());
        let mut fb_ctx = FunctionBuilderContext::new();
        let mut builder = FunctionBuilder::new(&mut ctx.func, &mut fb_ctx);

        // Stack-slot accumulators: one f64 per (group, agg) cell. Same
        // layout for grouped and ungrouped; ungrouped is just the
        // `n_groups == 1` degenerate case. Using stack slots (rather
        // than threading n_outputs block-params through the loop) keeps
        // the IR shape stable as n_outputs grows: Q1 has 30 cells
        // (5 groups × 6 aggs) which would be unwieldy as phi nodes.
        let mut acc_slots: Vec<StackSlot> = Vec::with_capacity(n_outputs);
        for _ in 0..n_outputs {
            // 8-byte f64 slot; cranelift 0.107 picks alignment from size
            // (no explicit align shift in this version's signature).
            let sd = StackSlotData::new(StackSlotKind::ExplicitSlot, 8);
            acc_slots.push(builder.create_sized_stack_slot(sd));
        }

        let entry = builder.create_block();
        let loop_header = builder.create_block();
        let loop_body = builder.create_block();
        let row_skip = builder.create_block();
        let loop_exit = builder.create_block();

        builder.append_block_params_for_function_params(entry);
        // loop_header carries only the row index (i: i64) now —
        // accumulators live in stack slots.
        builder.append_block_param(loop_header, types::I64);

        builder.switch_to_block(entry);
        builder.seal_block(entry);
        let p_n = builder.block_params(entry)[0];
        let p_inputs = builder.block_params(entry)[1];
        let p_outputs = builder.block_params(entry)[2];

        // Load each input column's base pointer once from inputs[k].
        let ptr_size = i64::from(ptr_ty.bytes());
        let mut col_ptrs: Vec<Value> = Vec::with_capacity(n_inputs);
        for k in 0..n_inputs {
            let off = builder.ins().iconst(types::I64, (k as i64) * ptr_size);
            let slot = builder.ins().iadd(p_inputs, off);
            let v = builder.ins().load(ptr_ty, MemFlags::trusted(), slot, 0);
            col_ptrs.push(v);
        }

        // Pre-seed each accumulator slot from outputs[k] (caller may
        // have set them to merge into a shared total across shards).
        let f64_size = i64::from(types::F64.bytes());
        for k in 0..n_outputs {
            let off = builder.ins().iconst(types::I64, (k as i64) * f64_size);
            let slot = builder.ins().iadd(p_outputs, off);
            let v = builder.ins().load(types::F64, MemFlags::trusted(), slot, 0);
            builder.ins().stack_store(v, acc_slots[k], 0);
        }

        let zero_i = builder.ins().iconst(types::I64, 0);
        builder.ins().jump(loop_header, &[zero_i]);

        // ----- Loop header: i < n ? body : exit -----
        builder.switch_to_block(loop_header);
        let i = builder.block_params(loop_header)[0];
        let cmp = builder.ins().icmp(IntCC::SignedLessThan, i, p_n);
        builder.ins().brif(cmp, loop_body, &[], loop_exit, &[]);

        // ----- Loop body: predicate eval -----
        builder.switch_to_block(loop_body);
        builder.seal_block(loop_body);

        let mut clause_masks: Vec<Value> = Vec::with_capacity(spec.predicate.len());
        for clause in &spec.predicate {
            let col_ty = spec.inputs[clause.column];
            let mask = emit_clause(&mut builder, col_ptrs[clause.column], col_ty, i, *clause);
            clause_masks.push(mask);
        }
        let pass_all = if clause_masks.is_empty() {
            builder.ins().iconst(types::I8, 1)
        } else {
            clause_masks
                .into_iter()
                .reduce(|a, b| builder.ins().band(a, b))
                .unwrap()
        };

        // Match path differs for grouped vs ungrouped. Both end with a
        // jump to row_skip.
        match spec.group.as_ref() {
            None => emit_match_path_ungrouped(
                &mut builder,
                &col_ptrs,
                &spec.inputs,
                &spec.aggregates,
                &acc_slots,
                i,
                pass_all,
                row_skip,
            ),
            Some(g) => emit_match_path_grouped(
                &mut builder,
                &col_ptrs,
                &spec.inputs,
                &spec.aggregates,
                g,
                &acc_slots,
                i,
                pass_all,
                row_skip,
            ),
        }

        // ----- Skip path: just increment i, jump back -----
        builder.switch_to_block(row_skip);
        builder.seal_block(row_skip);
        let next_i = builder.ins().iadd_imm(i, 1);
        builder.ins().jump(loop_header, &[next_i]);

        builder.seal_block(loop_header);

        // ----- Exit: store each accumulator slot back to outputs[k] -----
        builder.switch_to_block(loop_exit);
        builder.seal_block(loop_exit);
        for k in 0..n_outputs {
            let off = builder.ins().iconst(types::I64, (k as i64) * f64_size);
            let slot = builder.ins().iadd(p_outputs, off);
            let v = builder.ins().stack_load(types::F64, acc_slots[k], 0);
            builder.ins().store(MemFlags::trusted(), v, slot, 0);
        }
        builder.ins().return_(&[]);
        builder.finalize();

        // ----- 4. Verify + define + finalize -----
        verify_function(&ctx.func, module.isa()).map_err(|e| format!("verify_function: {e}"))?;
        module
            .define_function(func_id, &mut ctx)
            .map_err(|e| format!("define_function: {e}"))?;
        module.clear_context(&mut ctx);
        module
            .finalize_definitions()
            .map_err(|e| format!("finalize_definitions: {e}"))?;
        let func_ptr = module.get_finalized_function(func_id);
        Ok(Self {
            _module: module,
            func_ptr,
            n_inputs,
            n_outputs,
        })
    }

    /// Number of input columns the JIT'd function expects in `inputs[]`.
    pub fn n_inputs(&self) -> usize {
        self.n_inputs
    }
    /// Number of aggregate outputs the JIT'd function writes to `outputs[]`.
    pub fn n_outputs(&self) -> usize {
        self.n_outputs
    }

    /// Run the JIT'd function. `inputs` must point to an array of
    /// `self.n_inputs()` `*const u8` (each pointing at a column buffer
    /// with at least `n` elements). `outputs` must point to a buffer of
    /// `self.n_outputs()` f64 (caller initializes to 0 unless merging).
    ///
    /// # Safety
    /// - `inputs[k]` must be non-null, properly aligned for the spec's
    ///   `inputs[k]` element type, and have at least `n` elements.
    /// - `outputs` must be non-null, properly aligned f64, and have at
    ///   least `n_outputs()` elements.
    pub unsafe fn run(&self, n: i64, inputs: *const *const u8, outputs: *mut f64) {
        // SAFETY: caller upholds the contract documented above.
        unsafe {
            let func: FusedFilterAggFn = mem::transmute(self.func_ptr);
            func(n, inputs, outputs);
        }
    }
}

// SAFETY: same reasoning as Q6JitFn — emitted code reads only from input
// slices and writes to an out-buffer; no shared mutable state internally.
unsafe impl Send for FusedFilterAggJit {}
unsafe impl Sync for FusedFilterAggJit {}

// Cranelift's `JITModule` doesn't implement `Debug`, so we provide a
// minimal manual impl. This is needed because `FusedFilterSumExec`
// derives `Debug` and now holds an `Option<Arc<FusedFilterAggJit>>`.
impl std::fmt::Debug for FusedFilterAggJit {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FusedFilterAggJit")
            .field("n_inputs", &self.n_inputs)
            .field("n_outputs", &self.n_outputs)
            .field("func_ptr", &self.func_ptr)
            .finish_non_exhaustive()
    }
}

/// Emit the post-predicate match path for the ungrouped case (no
/// `GroupSpec`). For each aggregate `k`, compute its term at row `i`,
/// add to `acc_slots[k]` in place. Then jump to `row_skip`. The skip
/// branch (predicate-fail) jumps directly from `loop_body` to
/// `row_skip` without re-emitting anything here.
#[allow(clippy::too_many_arguments)]
fn emit_match_path_ungrouped(
    builder: &mut FunctionBuilder,
    col_ptrs: &[Value],
    col_tys: &[ColumnTy],
    aggregates: &[AggExpr],
    acc_slots: &[StackSlot],
    i: Value,
    pass_all: Value,
    row_skip: Block,
) {
    let match_block = builder.create_block();
    builder
        .ins()
        .brif(pass_all, match_block, &[], row_skip, &[]);

    builder.switch_to_block(match_block);
    builder.seal_block(match_block);
    for (k, agg) in aggregates.iter().enumerate() {
        let term = emit_agg_term(builder, col_ptrs, col_tys, i, agg);
        let cur = builder.ins().stack_load(types::F64, acc_slots[k], 0);
        let new = builder.ins().fadd(cur, term);
        builder.ins().stack_store(new, acc_slots[k], 0);
    }
    builder.ins().jump(row_skip, &[]);
}

/// Emit the post-predicate match path for the grouped case. After the
/// predicate passes, we:
/// 1. compute every agg term at row `i` once (Values used by every
///    group block),
/// 2. load the first-byte of each `group.key_columns[k]` at row `i`,
/// 3. branch through a chain of (key-tuple-match → group_g_body), with
///    the last fall-through going to the catch-all body,
/// 4. each `group_g_body` does `slot[g*n_aggs + k] += term[k]` for
///    every agg, then jumps to `row_skip`.
///
/// The pre-computed-terms approach mirrors the hand-coded Q1 inner
/// loop's order of fadds, preserving the bit-identical equivalence
/// property between JIT and hand-coded paths.
#[allow(clippy::too_many_arguments)]
fn emit_match_path_grouped(
    builder: &mut FunctionBuilder,
    col_ptrs: &[Value],
    col_tys: &[ColumnTy],
    aggregates: &[AggExpr],
    group: &GroupSpec,
    acc_slots: &[StackSlot],
    i: Value,
    pass_all: Value,
    row_skip: Block,
) {
    let n_aggs = aggregates.len();
    let n_known = group.known_keys.len();
    let catchall_idx = n_known;

    let setup_block = builder.create_block();
    builder
        .ins()
        .brif(pass_all, setup_block, &[], row_skip, &[]);

    // setup_block: compute terms + key bytes, then start the dispatch chain.
    builder.switch_to_block(setup_block);
    builder.seal_block(setup_block);

    let agg_terms: Vec<Value> = aggregates
        .iter()
        .map(|a| emit_agg_term(builder, col_ptrs, col_tys, i, a))
        .collect();
    let key_bytes: Vec<Value> = group
        .key_columns
        .iter()
        .map(|&c| emit_load_utf8view_first_byte(builder, col_ptrs[c], i))
        .collect();

    // Pre-create one body block per known group + one catch-all.
    let group_bodies: Vec<Block> = (0..=n_known).map(|_| builder.create_block()).collect();

    // Dispatch chain: a sequence of check_k blocks. check_0 starts in
    // setup_block (no extra block needed). For k > 0, create a new block.
    let mut check_blocks: Vec<Block> = vec![setup_block];
    for _ in 1..n_known {
        check_blocks.push(builder.create_block());
    }

    for (k, known) in group.known_keys.iter().enumerate() {
        if k > 0 {
            builder.switch_to_block(check_blocks[k]);
            builder.seal_block(check_blocks[k]);
        }
        // Build match_k = AND(key_bytes[j] == known[j] for j).
        let mask = build_key_match(builder, &key_bytes, known);
        let next = if k + 1 < n_known {
            check_blocks[k + 1]
        } else {
            // After the last known-key check, the fail edge lands at
            // the catch-all body.
            group_bodies[catchall_idx]
        };
        builder.ins().brif(mask, group_bodies[k], &[], next, &[]);
    }
    // If `n_known == 0` for some reason, every row goes to catchall
    // (the dispatch chain would be skipped). The setup_block in that
    // case still needs a terminator. We special-case here:
    if n_known == 0 {
        // setup_block is unterminated; jump to catchall body.
        builder.ins().jump(group_bodies[catchall_idx], &[]);
    }

    // Emit each group body (including catchall at index `n_known`).
    for (g, &body) in group_bodies.iter().enumerate() {
        builder.switch_to_block(body);
        builder.seal_block(body);
        for k in 0..n_aggs {
            let slot = acc_slots[g * n_aggs + k];
            let cur = builder.ins().stack_load(types::F64, slot, 0);
            let new = builder.ins().fadd(cur, agg_terms[k]);
            builder.ins().stack_store(new, slot, 0);
        }
        builder.ins().jump(row_skip, &[]);
    }
}

/// Build an i8 mask that's true iff every `key_bytes[j] == known[j]`.
/// Both sides are zero-extended to i32 for the comparison so the
/// integer-compare instruction matches the byte values cleanly.
fn build_key_match(builder: &mut FunctionBuilder, key_bytes: &[Value], known: &[u8]) -> Value {
    debug_assert_eq!(key_bytes.len(), known.len());
    let mut acc: Option<Value> = None;
    for (j, &b) in known.iter().enumerate() {
        let imm = builder.ins().iconst(types::I32, b as i64);
        let eq = builder.ins().icmp(IntCC::Equal, key_bytes[j], imm);
        acc = Some(match acc {
            None => eq,
            Some(prev) => builder.ins().band(prev, eq),
        });
    }
    // n_known per spec validation must be > 0 for grouped specs that
    // pass through this function; the loop above always assigns acc.
    acc.expect("build_key_match called with empty known-key tuple")
}

/// Load the first byte of a Utf8View element at row `i` and zero-
/// extend to i32. Arrow's StringView layout stores the inline data
/// starting at byte offset +4 within each 16-byte view.
fn emit_load_utf8view_first_byte(
    builder: &mut FunctionBuilder,
    col_base: Value,
    i: Value,
) -> Value {
    let view_size = builder.ins().iconst(types::I64, 16);
    let row_off = builder.ins().imul(i, view_size);
    let view_ptr = builder.ins().iadd(col_base, row_off);
    // Inline data starts at +4 (length is bytes 0..4).
    let byte_v = builder
        .ins()
        .load(types::I8, MemFlags::trusted(), view_ptr, 4);
    builder.ins().uextend(types::I32, byte_v)
}

/// Reject specs the emitter doesn't yet support, so the IR builder code
/// can rely on validated invariants (e.g. AggExpr columns are Float64).
fn validate_spec(spec: &FusedFilterAggSpec) -> Result<(), String> {
    for (i, c) in spec.predicate.iter().enumerate() {
        if c.column >= spec.inputs.len() {
            return Err(format!(
                "FusedFilterAggSpec: clause {i} references column {} but only {} inputs declared",
                c.column,
                spec.inputs.len()
            ));
        }
        let ct = spec.inputs[c.column];
        let ok = match c.op {
            ClauseOp::F64Ge | ClauseOp::F64Le | ClauseOp::F64Lt | ClauseOp::F64Gt => {
                ct == ColumnTy::Float64
            }
            ClauseOp::I32Ge | ClauseOp::I32Le | ClauseOp::I32Lt | ClauseOp::I32Gt => {
                ct == ColumnTy::Date32 || ct == ColumnTy::Int32
            }
        };
        if !ok {
            return Err(format!(
                "FusedFilterAggSpec: clause {i} op {:?} incompatible with column type {:?}",
                c.op, ct
            ));
        }
    }
    for (i, a) in spec.aggregates.iter().enumerate() {
        // Validate Float64 columns by collecting them per-variant.
        // Some variants also have a Utf8View guard column with its own
        // type/length constraints; we handle those separately below.
        let (f64_cols, guard_constraint): (Vec<usize>, Option<(usize, usize)>) = match a {
            AggExpr::SumColumn(c) => (vec![*c], None),
            AggExpr::SumProductColumns(a, b) => {
                if a == b {
                    return Err(format!(
                        "FusedFilterAggSpec: aggregate {i} multiplies a column by itself"
                    ));
                }
                (vec![*a, *b], None)
            }
            AggExpr::SumProductOneMinus(a, b) => (vec![*a, *b], None),
            AggExpr::SumProductTwoOneMinusOnePlus(a, b, c) => (vec![*a, *b, *c], None),
            AggExpr::CountStar => (vec![], None),
            AggExpr::SumProductOneMinusGuardedByPrefix {
                guard_col,
                val_col,
                disc_col,
                prefix,
            } => {
                if prefix.is_empty() {
                    return Err(format!(
                        "FusedFilterAggSpec: aggregate {i} has empty guard prefix"
                    ));
                }
                if prefix.len() > 4 {
                    return Err(format!(
                        "FusedFilterAggSpec: aggregate {i} guard prefix has {} bytes; the JIT \
                         can only access the first 4 bytes of a Utf8View element reliably \
                         (bytes 5+ of long strings live in an external buffer). Reduce the \
                         prefix to ≤4 bytes that uniquely identify the match.",
                        prefix.len()
                    ));
                }
                (vec![*val_col, *disc_col], Some((*guard_col, prefix.len())))
            }
        };
        for c in f64_cols {
            if c >= spec.inputs.len() {
                return Err(format!(
                    "FusedFilterAggSpec: aggregate {i} references column {c} but only {} inputs",
                    spec.inputs.len()
                ));
            }
            if spec.inputs[c] != ColumnTy::Float64 {
                return Err(format!(
                    "FusedFilterAggSpec: aggregate {i} column {c} must be Float64, got {:?}",
                    spec.inputs[c]
                ));
            }
        }
        if let Some((gc, _)) = guard_constraint {
            if gc >= spec.inputs.len() {
                return Err(format!(
                    "FusedFilterAggSpec: aggregate {i} guard column {gc} out of range ({} inputs)",
                    spec.inputs.len()
                ));
            }
            if spec.inputs[gc] != ColumnTy::Utf8View {
                return Err(format!(
                    "FusedFilterAggSpec: aggregate {i} guard column {gc} must be Utf8View, got {:?}",
                    spec.inputs[gc]
                ));
            }
        }
    }
    if let Some(g) = &spec.group {
        if g.key_columns.is_empty() {
            return Err("FusedFilterAggSpec: GroupSpec has zero key columns".into());
        }
        if g.known_keys.is_empty() {
            return Err(
                "FusedFilterAggSpec: GroupSpec has zero known_keys — would route all rows to catchall and emit no groups".into(),
            );
        }
        for &c in &g.key_columns {
            if c >= spec.inputs.len() {
                return Err(format!(
                    "FusedFilterAggSpec: group key column {c} out of range ({} inputs)",
                    spec.inputs.len()
                ));
            }
            if spec.inputs[c] != ColumnTy::Utf8View {
                return Err(format!(
                    "FusedFilterAggSpec: group key column {c} must be Utf8View, got {:?}",
                    spec.inputs[c]
                ));
            }
        }
        for (i, key) in g.known_keys.iter().enumerate() {
            if key.len() != g.key_columns.len() {
                return Err(format!(
                    "FusedFilterAggSpec: known_keys[{i}] has {} bytes but key_columns has {}",
                    key.len(),
                    g.key_columns.len()
                ));
            }
        }
    }
    Ok(())
}

/// Emit IR for `column[i] OP imm`, returning an i8 boolean mask.
fn emit_clause(
    builder: &mut FunctionBuilder,
    col_base: Value,
    col_ty: ColumnTy,
    i: Value,
    clause: Clause,
) -> Value {
    let elem_size = builder.ins().iconst(types::I64, col_ty.elem_size());
    let off = builder.ins().imul(i, elem_size);
    let slot = builder.ins().iadd(col_base, off);
    match clause.op {
        ClauseOp::F64Ge | ClauseOp::F64Le | ClauseOp::F64Lt | ClauseOp::F64Gt => {
            let v = builder.ins().load(types::F64, MemFlags::trusted(), slot, 0);
            let imm = builder.ins().f64const(clause.imm_f64);
            let cc = match clause.op {
                ClauseOp::F64Ge => FloatCC::GreaterThanOrEqual,
                ClauseOp::F64Le => FloatCC::LessThanOrEqual,
                ClauseOp::F64Lt => FloatCC::LessThan,
                ClauseOp::F64Gt => FloatCC::GreaterThan,
                _ => unreachable!(),
            };
            builder.ins().fcmp(cc, v, imm)
        }
        ClauseOp::I32Ge | ClauseOp::I32Le | ClauseOp::I32Lt | ClauseOp::I32Gt => {
            let v = builder.ins().load(types::I32, MemFlags::trusted(), slot, 0);
            let imm = builder.ins().iconst(types::I32, i64::from(clause.imm_i32));
            let cc = match clause.op {
                ClauseOp::I32Ge => IntCC::SignedGreaterThanOrEqual,
                ClauseOp::I32Le => IntCC::SignedLessThanOrEqual,
                ClauseOp::I32Lt => IntCC::SignedLessThan,
                ClauseOp::I32Gt => IntCC::SignedGreaterThan,
                _ => unreachable!(),
            };
            builder.ins().icmp(cc, v, imm)
        }
    }
}

/// Emit IR for one `AggExpr` evaluated at row `i`, returning the f64
/// contribution (already validated as a Float64 column or product).
fn emit_agg_term(
    builder: &mut FunctionBuilder,
    col_ptrs: &[Value],
    col_tys: &[ColumnTy],
    i: Value,
    agg: &AggExpr,
) -> Value {
    let load_f64 = |b: &mut FunctionBuilder, col: usize| -> Value {
        debug_assert_eq!(col_tys[col], ColumnTy::Float64);
        let elem_size = b.ins().iconst(types::I64, 8);
        let off = b.ins().imul(i, elem_size);
        let slot = b.ins().iadd(col_ptrs[col], off);
        b.ins().load(types::F64, MemFlags::trusted(), slot, 0)
    };
    match agg {
        AggExpr::SumColumn(c) => load_f64(builder, *c),
        AggExpr::SumProductColumns(a, b_idx) => {
            let av = load_f64(builder, *a);
            let bv = load_f64(builder, *b_idx);
            builder.ins().fmul(av, bv)
        }
        AggExpr::SumProductOneMinus(a, b_idx) => {
            // term = col[a] * (1 - col[b])
            let av = load_f64(builder, *a);
            let bv = load_f64(builder, *b_idx);
            let one = builder.ins().f64const(1.0);
            let om = builder.ins().fsub(one, bv);
            builder.ins().fmul(av, om)
        }
        AggExpr::SumProductTwoOneMinusOnePlus(a, b_idx, c_idx) => {
            // term = col[a] * (1 - col[b]) * (1 + col[c])
            let av = load_f64(builder, *a);
            let bv = load_f64(builder, *b_idx);
            let cv = load_f64(builder, *c_idx);
            let one_b = builder.ins().f64const(1.0);
            let one_c = builder.ins().f64const(1.0);
            let om = builder.ins().fsub(one_b, bv);
            let op = builder.ins().fadd(one_c, cv);
            let av_om = builder.ins().fmul(av, om);
            builder.ins().fmul(av_om, op)
        }
        AggExpr::CountStar => builder.ins().f64const(1.0),
        AggExpr::SumProductOneMinusGuardedByPrefix {
            guard_col,
            val_col,
            disc_col,
            prefix,
        } => {
            // Compute the unguarded term first.
            let av = load_f64(builder, *val_col);
            let bv = load_f64(builder, *disc_col);
            let one = builder.ins().f64const(1.0);
            let om = builder.ins().fsub(one, bv);
            let term = builder.ins().fmul(av, om);
            // Prefix match on the Utf8View's inline bytes (offset +4).
            let prefix_match = emit_utf8view_prefix_match(builder, col_ptrs[*guard_col], i, prefix);
            // Branchless: pick `term` if prefix matched, else 0.0.
            let zero = builder.ins().f64const(0.0);
            builder.ins().select(prefix_match, term, zero)
        }
    }
}

/// Emit IR for "first `prefix.len()` bytes of the Utf8View at row `i`
/// equal `prefix`". Returns an i8 boolean mask. Each prefix byte is a
/// separate u8 load + i32 compare; we AND them. Restricted to ≤4
/// bytes because the StringView non-inline layout only puts the first
/// 4 string bytes at view offset +4; bytes 5+ for long strings live in
/// an external buffer the IR can't reach without a host callback.
fn emit_utf8view_prefix_match(
    builder: &mut FunctionBuilder,
    col_base: Value,
    i: Value,
    prefix: &[u8],
) -> Value {
    debug_assert!(!prefix.is_empty(), "validated > 0 above");
    debug_assert!(prefix.len() <= 4, "validated ≤4 bytes above");
    // view_ptr = col_base + 16 * i
    let view_size = builder.ins().iconst(types::I64, 16);
    let row_off = builder.ins().imul(i, view_size);
    let view_ptr = builder.ins().iadd(col_base, row_off);
    let mut acc: Option<Value> = None;
    for (j, &b) in prefix.iter().enumerate() {
        // Byte j of the inline data is at view + 4 + j.
        let load = builder
            .ins()
            .load(types::I8, MemFlags::trusted(), view_ptr, 4 + j as i32);
        let loaded = builder.ins().uextend(types::I32, load);
        let imm = builder.ins().iconst(types::I32, b as i64);
        let eq = builder.ins().icmp(IntCC::Equal, loaded, imm);
        acc = Some(match acc {
            None => eq,
            Some(prev) => builder.ins().band(prev, eq),
        });
    }
    acc.unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn q6_jit_matches_reference_on_synthetic_batch() {
        // Build a 6-row synthetic batch. Same pattern as the Σ.D1
        // unit test — one matching row, five mismatched.
        //   row 0: match → contributes 100.0 * 0.06 = 6.0
        //   rows 1-5: each fails one predicate clause
        let shipdate: Vec<i32> = vec![8800, 8000, 9500, 8800, 8800, 8800];
        let discount: Vec<f64> = vec![0.06, 0.06, 0.06, 0.04, 0.08, 0.06];
        let quantity: Vec<f64> = vec![10.0, 10.0, 10.0, 10.0, 10.0, 24.0];
        let extprice: Vec<f64> = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0];

        let jit =
            Q6JitFn::try_build_q6_canonical().expect("Q6 JIT build should succeed on this host");

        let mut running: f64 = 0.0;
        // SAFETY: pointers backed by stack-owned Vec<T>, all len >= n.
        unsafe {
            jit.run(
                shipdate.len() as i64,
                shipdate.as_ptr(),
                discount.as_ptr(),
                quantity.as_ptr(),
                extprice.as_ptr(),
                &mut running,
            );
        }
        assert!(
            (running - 6.0).abs() < 1e-9,
            "JIT'd Q6 predicate produced {running}, expected 6.0",
        );
    }

    #[test]
    fn q6_jit_caller_preseeded_sum_is_added_to() {
        // Verifies the JIT loads *out_sum at entry — useful for
        // cross-shard merging (each shard adds to a shared total).
        let shipdate: Vec<i32> = vec![8800, 8800];
        let discount: Vec<f64> = vec![0.06, 0.06];
        let quantity: Vec<f64> = vec![10.0, 10.0];
        let extprice: Vec<f64> = vec![100.0, 100.0];

        let jit = Q6JitFn::try_build_q6_canonical().unwrap();
        let mut running: f64 = 100.0; // pre-seeded
        // SAFETY: same as above.
        unsafe {
            jit.run(
                shipdate.len() as i64,
                shipdate.as_ptr(),
                discount.as_ptr(),
                quantity.as_ptr(),
                extprice.as_ptr(),
                &mut running,
            );
        }
        // 100 (pre-seed) + 2 * (100 * 0.06) = 100 + 12 = 112
        assert!(
            (running - 112.0).abs() < 1e-9,
            "expected pre-seeded 100 + 12 = 112, got {running}",
        );
    }

    #[test]
    fn q6_jit_parametrized_bounds_round_trip() {
        // Build with non-canonical bounds and check the rows that
        // match those bounds are accumulated correctly.
        let shipdate: Vec<i32> = vec![100, 200, 300];
        let discount: Vec<f64> = vec![0.10, 0.10, 0.10];
        let quantity: Vec<f64> = vec![1.0, 1.0, 1.0];
        let extprice: Vec<f64> = vec![10.0, 20.0, 30.0];

        // ship in [150, 250), disc in [0.05, 0.15], qty < 100
        let jit = Q6JitFn::try_build(150, 250, 0.05, 0.15, 100.0).unwrap();
        let mut running: f64 = 0.0;
        // SAFETY: same as above.
        unsafe {
            jit.run(
                shipdate.len() as i64,
                shipdate.as_ptr(),
                discount.as_ptr(),
                quantity.as_ptr(),
                extprice.as_ptr(),
                &mut running,
            );
        }
        // Only row 1 matches: 20.0 * 0.10 = 2.0
        assert!(
            (running - 2.0).abs() < 1e-9,
            "expected 2.0 (only row 1 matched), got {running}",
        );
    }

    // ----- Phase A: spec-driven generic emitter -----
    //
    // These tests establish two invariants:
    //   1. The generic emitter produces the same result as the Q6
    //      hand-coded path for Q6's spec ("equivalence").
    //   2. The emitter handles a second shape (single SumColumn aggregate,
    //      no products) so we know the spec walk is data-driven and not
    //      accidentally hard-coding Q6.

    /// Helper: invoke the generic JIT on a Q6-shaped batch via the
    /// fixed-arity `inputs/outputs` array form.
    fn run_generic_q6(
        jit: &FusedFilterAggJit,
        shipdate: &[i32],
        discount: &[f64],
        quantity: &[f64],
        extprice: &[f64],
    ) -> f64 {
        let inputs: [*const u8; 4] = [
            shipdate.as_ptr().cast::<u8>(),
            discount.as_ptr().cast::<u8>(),
            quantity.as_ptr().cast::<u8>(),
            extprice.as_ptr().cast::<u8>(),
        ];
        let mut outputs: [f64; 1] = [0.0];
        // SAFETY: inputs all have length >= n; outputs has len >= 1; pointer
        // alignment is upheld by the source slices' element type.
        unsafe {
            jit.run(shipdate.len() as i64, inputs.as_ptr(), outputs.as_mut_ptr());
        }
        outputs[0]
    }

    #[test]
    fn generic_jit_q6_spec_matches_q6_jit_on_synthetic_batch() {
        // Same 6-row batch as the hand-coded Q6 JIT test above; result
        // must be byte-identical (single matching row contributes 6.0).
        let shipdate: Vec<i32> = vec![8800, 8000, 9500, 8800, 8800, 8800];
        let discount: Vec<f64> = vec![0.06, 0.06, 0.06, 0.04, 0.08, 0.06];
        let quantity: Vec<f64> = vec![10.0, 10.0, 10.0, 10.0, 10.0, 24.0];
        let extprice: Vec<f64> = vec![100.0, 100.0, 100.0, 100.0, 100.0, 100.0];

        let spec = FusedFilterAggSpec::q6(8766, 9131, 0.05, 0.07, 24.0);
        let jit = FusedFilterAggJit::try_build(&spec).expect("build generic JIT");
        let got = run_generic_q6(&jit, &shipdate, &discount, &quantity, &extprice);
        assert!(
            (got - 6.0).abs() < 1e-9,
            "generic JIT produced {got}, expected 6.0",
        );
    }

    #[test]
    fn generic_jit_q6_spec_matches_hand_coded_q6_jit_byte_identical() {
        // Cross-check: same data through both JIT paths should produce
        // bit-identical f64 sums (no FP-order surprises since the loop
        // walks rows left-to-right in both).
        let shipdate: Vec<i32> = vec![8800, 9000, 8700, 8900];
        let discount: Vec<f64> = vec![0.06, 0.05, 0.07, 0.06];
        let quantity: Vec<f64> = vec![10.0, 23.0, 10.0, 22.0];
        let extprice: Vec<f64> = vec![100.0, 200.0, 300.0, 50.0];

        // Hand-coded Q6 path.
        let hand_jit = Q6JitFn::try_build_q6_canonical().unwrap();
        let mut hand_sum: f64 = 0.0;
        // SAFETY: all slices >= n, ptr aligned by source type.
        unsafe {
            hand_jit.run(
                shipdate.len() as i64,
                shipdate.as_ptr(),
                discount.as_ptr(),
                quantity.as_ptr(),
                extprice.as_ptr(),
                &mut hand_sum,
            );
        }
        // Spec-driven path.
        let spec = FusedFilterAggSpec::q6(8766, 9131, 0.05, 0.07, 24.0);
        let generic = FusedFilterAggJit::try_build(&spec).unwrap();
        let got = run_generic_q6(&generic, &shipdate, &discount, &quantity, &extprice);
        assert_eq!(
            hand_sum.to_bits(),
            got.to_bits(),
            "spec-driven Q6 JIT returned {got}, hand-coded returned {hand_sum} \
             (must be bit-identical — same row order, same FP ops)",
        );
    }

    /// SumColumn variant exercises the path the SumProductColumns case
    /// doesn't: aggregate over a single column rather than a product.
    #[test]
    fn generic_jit_single_sum_column() {
        // Spec: one Float64 input, no predicate clauses, SUM(col 0).
        // Result should equal the unconditional sum of the data.
        let spec = FusedFilterAggSpec {
            inputs: vec![ColumnTy::Float64],
            predicate: vec![],
            aggregates: vec![AggExpr::SumColumn(0)],
            group: None,
        };
        let jit = FusedFilterAggJit::try_build(&spec).expect("build");
        let data: Vec<f64> = vec![1.0, 2.0, 4.0, 8.0, 16.0];
        let inputs: [*const u8; 1] = [data.as_ptr().cast::<u8>()];
        let mut outputs: [f64; 1] = [0.0];
        // SAFETY: data.len() == n, single Float64 input.
        unsafe {
            jit.run(data.len() as i64, inputs.as_ptr(), outputs.as_mut_ptr());
        }
        assert!(
            (outputs[0] - 31.0).abs() < 1e-9,
            "expected sum 31.0, got {}",
            outputs[0],
        );
    }

    /// Multiple-aggregate variant verifies the IR's per-aggregate
    /// accumulator threading works for n_outputs > 1.
    #[test]
    fn generic_jit_two_aggregates_share_one_pass() {
        // Spec: two inputs (Float64, Float64), no predicate.
        // Agg 0: SUM(col 0). Agg 1: SUM(col 0 * col 1).
        let spec = FusedFilterAggSpec {
            inputs: vec![ColumnTy::Float64, ColumnTy::Float64],
            predicate: vec![],
            aggregates: vec![AggExpr::SumColumn(0), AggExpr::SumProductColumns(0, 1)],
            group: None,
        };
        let jit = FusedFilterAggJit::try_build(&spec).expect("build");
        let a: Vec<f64> = vec![1.0, 2.0, 3.0];
        let b: Vec<f64> = vec![10.0, 20.0, 30.0];
        let inputs: [*const u8; 2] = [a.as_ptr().cast::<u8>(), b.as_ptr().cast::<u8>()];
        let mut outputs: [f64; 2] = [0.0, 0.0];
        // SAFETY: a.len()==b.len()==n, both Float64.
        unsafe {
            jit.run(a.len() as i64, inputs.as_ptr(), outputs.as_mut_ptr());
        }
        // SUM(a) = 6, SUM(a*b) = 10 + 40 + 90 = 140.
        assert!((outputs[0] - 6.0).abs() < 1e-9, "got {}", outputs[0]);
        assert!((outputs[1] - 140.0).abs() < 1e-9, "got {}", outputs[1]);
    }

    /// Phase B exercise: the Q1 spec (Utf8View group keys, 5 SUMs +
    /// COUNT per group, Date32 predicate) must compute per-group
    /// accumulators that match the same math the hand-coded Q1 inner
    /// loop does for the same synthetic data.
    ///
    /// Synthetic batch (mirrors `fused_multi_agg.rs`'s test layout):
    ///   3 rows in (N,F): qty=10, price=100, disc=0.05, tax=0.10, ship=8800 (in-range)
    ///   1 row  in (A,F): qty=20, price=200, disc=0.10, tax=0.05, ship=8800 (in-range)
    ///   1 row  in (R,F) but ship=10472 (out of range — predicate fails)
    /// Expected per-group accumulators (sum_qty, sum_price,
    /// sum_disc_price, sum_charge, sum_disc, count):
    ///   (N,F) [group 1]: 30, 300, 285, 313.5, 0.15, 3
    ///   (A,F) [group 3]: 20, 200, 180, 189, 0.10, 1
    ///   (R,F) [group 0]: 0, 0, 0, 0, 0, 0
    ///   (N,O) [group 2]: 0, 0, 0, 0, 0, 0
    ///   catch-all [group 4]: 0, 0, 0, 0, 0, 0
    #[test]
    fn generic_jit_q1_spec_groups_match_hand_math() {
        // Build the inputs as 16-byte Utf8View slots: byte 0..4 = length=1,
        // bytes 4..16 = inline data (only byte 4 matters for our dispatch).
        fn view_of(c: u8) -> [u8; 16] {
            let mut v = [0u8; 16];
            v[0] = 1; // length = 1 (low-order byte of u32 length)
            v[4] = c;
            v
        }
        let rflag_views: Vec<[u8; 16]> = vec![
            view_of(b'N'),
            view_of(b'N'),
            view_of(b'N'),
            view_of(b'A'),
            view_of(b'R'),
        ];
        let lstatus_views: Vec<[u8; 16]> = vec![
            view_of(b'F'),
            view_of(b'F'),
            view_of(b'F'),
            view_of(b'F'),
            view_of(b'F'),
        ];
        let qty: Vec<f64> = vec![10.0, 10.0, 10.0, 20.0, 5.0];
        let price: Vec<f64> = vec![100.0, 100.0, 100.0, 200.0, 50.0];
        let disc: Vec<f64> = vec![0.05, 0.05, 0.05, 0.10, 0.02];
        let tax: Vec<f64> = vec![0.10, 0.10, 0.10, 0.05, 0.05];
        let cutoff: i32 = 10471;
        let ship: Vec<i32> = vec![8800, 8800, 8800, 8800, cutoff + 1];

        let spec = FusedFilterAggSpec::q1(cutoff);
        let jit = FusedFilterAggJit::try_build(&spec).expect("build Q1 JIT");

        let inputs: [*const u8; 7] = [
            rflag_views.as_ptr().cast::<u8>(),
            lstatus_views.as_ptr().cast::<u8>(),
            qty.as_ptr().cast::<u8>(),
            price.as_ptr().cast::<u8>(),
            disc.as_ptr().cast::<u8>(),
            tax.as_ptr().cast::<u8>(),
            ship.as_ptr().cast::<u8>(),
        ];
        // 5 groups × 6 aggs = 30 cells, all zero-initialized.
        let mut outputs: [f64; 30] = [0.0; 30];
        // SAFETY: all input buffers have at least n=5 elements; outputs
        // holds exactly 30 f64 cells matching `jit.n_outputs()`.
        assert_eq!(jit.n_outputs(), 30);
        unsafe {
            jit.run(ship.len() as i64, inputs.as_ptr(), outputs.as_mut_ptr());
        }

        // (N,F) is group 1 in q1's known_keys order. n_aggs=6 cells per group.
        let nf = &outputs[1 * 6..1 * 6 + 6];
        assert!((nf[0] - 30.0).abs() < 1e-9, "(N,F) sum_qty: {nf:?}");
        assert!((nf[1] - 300.0).abs() < 1e-9, "(N,F) sum_price: {nf:?}");
        let nf_disc_price = 300.0 * (1.0 - 0.05); // 285
        assert!(
            (nf[2] - nf_disc_price).abs() < 1e-6,
            "(N,F) sum_disc_price: {nf:?}"
        );
        let nf_charge = nf_disc_price * (1.0 + 0.10); // 313.5
        assert!((nf[3] - nf_charge).abs() < 1e-6, "(N,F) sum_charge: {nf:?}");
        assert!((nf[4] - 0.15).abs() < 1e-9, "(N,F) sum_disc: {nf:?}");
        assert!((nf[5] - 3.0).abs() < 1e-9, "(N,F) count: {nf:?}");

        // (A,F) is group 3.
        let af = &outputs[3 * 6..3 * 6 + 6];
        assert!((af[0] - 20.0).abs() < 1e-9, "(A,F) sum_qty: {af:?}");
        assert!((af[5] - 1.0).abs() < 1e-9, "(A,F) count: {af:?}");

        // (R,F) row was filtered out by the shipdate predicate, so its
        // group cells (group 0) must remain zero.
        let rf = &outputs[0 * 6..0 * 6 + 6];
        assert!(
            rf.iter().all(|v| *v == 0.0),
            "(R,F) shouldn't accumulate: {rf:?}"
        );

        // Catch-all (group 4) must also be zero — no rows had unknown keys.
        let catch = &outputs[4 * 6..4 * 6 + 6];
        assert!(catch.iter().all(|v| *v == 0.0), "catch-all: {catch:?}");
    }

    /// Phase C exercise: Q14 post-join spec (CASE-WHEN guard, dual SUM,
    /// no group-by) computes promo+total cells that match hand math.
    ///
    /// Synthetic batch (the spec uses a 4-byte `"PROM"` prefix — see
    /// `q14_post_join` docs for why):
    ///   row 0: p_type="PROMO BRUSH",      price=100, disc=0.10 → guarded=90,  unguarded=90
    ///   row 1: p_type="PROMO POLIS",      price=50,  disc=0.00 → guarded=50,  unguarded=50
    ///   row 2: p_type="ECONOMY ANO",      price=200, disc=0.20 → guarded=0,   unguarded=160
    ///   row 3: p_type="STANDARD PO",      price=100, disc=0.50 → guarded=0,   unguarded=50
    /// Expected: promo=140, total=350; ratio (host-computed) = 100*140/350 ≈ 40.0%
    #[test]
    fn generic_jit_q14_post_join_spec_matches_hand_math() {
        fn view_of(s: &str) -> [u8; 16] {
            let bytes = s.as_bytes();
            let mut v = [0u8; 16];
            // length stored as u32 LE in bytes 0..4
            let n = bytes.len().min(12) as u32;
            v[0..4].copy_from_slice(&n.to_le_bytes());
            // inline data at bytes 4..(4+n)
            v[4..4 + n as usize].copy_from_slice(&bytes[..n as usize]);
            v
        }
        let ptype_views: Vec<[u8; 16]> = vec![
            view_of("PROMO BRUSH"), // 11 bytes — fits inline
            view_of("PROMO POLIS"), // also fits
            view_of("ECONOMY ANO"),
            view_of("STANDARD PO"),
        ];
        let price: Vec<f64> = vec![100.0, 50.0, 200.0, 100.0];
        let disc: Vec<f64> = vec![0.10, 0.00, 0.20, 0.50];

        let spec = FusedFilterAggSpec::q14_post_join();
        let jit = FusedFilterAggJit::try_build(&spec).expect("build Q14 JIT");
        let inputs: [*const u8; 3] = [
            ptype_views.as_ptr().cast::<u8>(),
            price.as_ptr().cast::<u8>(),
            disc.as_ptr().cast::<u8>(),
        ];
        let mut outputs: [f64; 2] = [0.0, 0.0];
        // SAFETY: ptype_views/price/disc each have n=4 elements; outputs
        // has 2 cells matching jit.n_outputs().
        assert_eq!(jit.n_outputs(), 2);
        unsafe {
            jit.run(price.len() as i64, inputs.as_ptr(), outputs.as_mut_ptr());
        }
        let promo = outputs[0];
        let total = outputs[1];
        assert!((promo - 140.0).abs() < 1e-6, "promo: {promo}");
        assert!((total - 350.0).abs() < 1e-6, "total: {total}");
        let ratio = 100.0 * promo / total;
        assert!((ratio - 40.0).abs() < 1e-9, "ratio: {ratio}");
    }

    #[test]
    fn generic_jit_rejects_clause_referencing_oob_column() {
        let spec = FusedFilterAggSpec {
            inputs: vec![ColumnTy::Float64],
            predicate: vec![Clause {
                column: 5, // oob — only 1 input
                op: ClauseOp::F64Lt,
                imm_i32: 0,
                imm_f64: 0.0,
            }],
            aggregates: vec![AggExpr::SumColumn(0)],
            group: None,
        };
        // The Ok variant doesn't impl Debug, so we can't use unwrap_err().
        match FusedFilterAggJit::try_build(&spec) {
            Err(e) => assert!(e.contains("clause 0"), "got: {e}"),
            Ok(_) => panic!("expected validation to reject oob column"),
        }
    }

    #[test]
    fn generic_jit_rejects_clause_type_mismatch() {
        // I32Lt clause on a Float64 column — must reject.
        let spec = FusedFilterAggSpec {
            inputs: vec![ColumnTy::Float64],
            predicate: vec![Clause {
                column: 0,
                op: ClauseOp::I32Lt,
                imm_i32: 100,
                imm_f64: 0.0,
            }],
            aggregates: vec![AggExpr::SumColumn(0)],
            group: None,
        };
        match FusedFilterAggJit::try_build(&spec) {
            Err(e) => assert!(e.contains("incompatible"), "got: {e}"),
            Ok(_) => panic!("expected validation to reject type mismatch"),
        }
    }
}
