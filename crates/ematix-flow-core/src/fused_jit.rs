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
use cranelift_codegen::ir::{AbiParam, Function, InstBuilder, Signature, UserFuncName};
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
}

impl ColumnTy {
    /// Byte width of one element in the column buffer.
    fn elem_size(self) -> i64 {
        match self {
            ColumnTy::Float64 | ColumnTy::Int64 => 8,
            ColumnTy::Date32 | ColumnTy::Int32 => 4,
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
/// is always f64 (DataFusion's SUM-of-numeric default). One `AggExpr`
/// produces one entry in the JIT'd function's `outputs[]` array.
#[derive(Debug, Clone, Copy)]
pub enum AggExpr {
    /// `SUM(col[i])` — column must be Float64.
    SumColumn(usize),
    /// `SUM(col[a] * col[b])` — both columns must be Float64. Q6's
    /// `SUM(l_extendedprice * l_discount)` shape.
    SumProductColumns(usize, usize),
}

/// Top-level spec. Owns the slots all three families (predicate, aggs,
/// inputs) describe by index. Validated at JIT-build time.
#[derive(Debug, Clone, Default)]
pub struct FusedFilterAggSpec {
    pub inputs: Vec<ColumnTy>,
    pub predicate: Vec<Clause>,
    pub aggregates: Vec<AggExpr>,
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
        let n_outputs = spec.aggregates.len();

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

        let entry = builder.create_block();
        let loop_header = builder.create_block();
        let loop_body = builder.create_block();
        let row_match = builder.create_block();
        let row_skip = builder.create_block();
        let loop_exit = builder.create_block();

        builder.append_block_params_for_function_params(entry);

        // loop_header carries: i (i64) + one f64 accumulator per aggregate.
        builder.append_block_param(loop_header, types::I64);
        for _ in 0..n_outputs {
            builder.append_block_param(loop_header, types::F64);
        }

        builder.switch_to_block(entry);
        builder.seal_block(entry);
        let p_n = builder.block_params(entry)[0];
        let p_inputs = builder.block_params(entry)[1];
        let p_outputs = builder.block_params(entry)[2];

        // Load each input column's base pointer once from inputs[k]. We
        // store these in a Vec<Value> indexed by column-spec order — the
        // hot loop references col_ptrs[clause.column] / [agg.column].
        let ptr_size = i64::from(ptr_ty.bytes());
        let mut col_ptrs: Vec<Value> = Vec::with_capacity(n_inputs);
        for k in 0..n_inputs {
            let off = builder.ins().iconst(types::I64, (k as i64) * ptr_size);
            let slot = builder.ins().iadd(p_inputs, off);
            let v = builder
                .ins()
                .load(ptr_ty, MemFlags::trusted(), slot, 0);
            col_ptrs.push(v);
        }

        // Pre-seed accumulators from outputs[k] (caller may have set
        // them to merge into a shared total across shards).
        let f64_size = i64::from(types::F64.bytes());
        let mut init_sums: Vec<Value> = Vec::with_capacity(n_outputs);
        for k in 0..n_outputs {
            let off = builder
                .ins()
                .iconst(types::I64, (k as i64) * f64_size);
            let slot = builder.ins().iadd(p_outputs, off);
            let v = builder.ins().load(types::F64, MemFlags::trusted(), slot, 0);
            init_sums.push(v);
        }

        let zero_i = builder.ins().iconst(types::I64, 0);
        let mut entry_args = Vec::with_capacity(1 + n_outputs);
        entry_args.push(zero_i);
        entry_args.extend(init_sums.iter().copied());
        builder.ins().jump(loop_header, &entry_args);

        // ----- Loop header: i < n ? body : exit -----
        builder.switch_to_block(loop_header);
        let i = builder.block_params(loop_header)[0];
        // Snapshot the running-sum block-param IDs (avoid borrow conflicts
        // with `builder.ins()` inside the loop body).
        let header_sums: Vec<Value> =
            (0..n_outputs).map(|k| builder.block_params(loop_header)[1 + k]).collect();
        let cmp = builder.ins().icmp(IntCC::SignedLessThan, i, p_n);
        builder.ins().brif(cmp, loop_body, &[], loop_exit, &[]);

        // ----- Loop body: emit predicate eval, accumulate sums for match -----
        builder.switch_to_block(loop_body);
        builder.seal_block(loop_body);

        // Evaluate each clause, AND them. Each clause loads its column
        // at column[i], compares to the immediate, produces an i8 (0/1)
        // mask.
        let mut clause_masks: Vec<Value> = Vec::with_capacity(spec.predicate.len());
        for clause in &spec.predicate {
            let col_ty = spec.inputs[clause.column];
            let mask =
                emit_clause(&mut builder, col_ptrs[clause.column], col_ty, i, *clause);
            clause_masks.push(mask);
        }
        // AND all masks. If the predicate is empty (rare — every row
        // passes), we use a constant true.
        let pass_all = if clause_masks.is_empty() {
            builder.ins().iconst(types::I8, 1)
        } else {
            clause_masks
                .into_iter()
                .reduce(|a, b| builder.ins().band(a, b))
                .unwrap()
        };
        builder.ins().brif(pass_all, row_match, &[], row_skip, &[]);

        // ----- Match path: compute each AggExpr, add to its accumulator -----
        builder.switch_to_block(row_match);
        builder.seal_block(row_match);
        let mut new_sums: Vec<Value> = Vec::with_capacity(n_outputs);
        for (k, agg) in spec.aggregates.iter().enumerate() {
            let term = emit_agg_term(&mut builder, &col_ptrs, &spec.inputs, i, *agg);
            let new = builder.ins().fadd(header_sums[k], term);
            new_sums.push(new);
        }
        let next_i = builder.ins().iadd_imm(i, 1);
        let mut match_args = Vec::with_capacity(1 + n_outputs);
        match_args.push(next_i);
        match_args.extend(new_sums.iter().copied());
        builder.ins().jump(loop_header, &match_args);

        // ----- Skip path: pass sums through unchanged -----
        builder.switch_to_block(row_skip);
        builder.seal_block(row_skip);
        let next_i_skip = builder.ins().iadd_imm(i, 1);
        let mut skip_args = Vec::with_capacity(1 + n_outputs);
        skip_args.push(next_i_skip);
        skip_args.extend(header_sums.iter().copied());
        builder.ins().jump(loop_header, &skip_args);

        builder.seal_block(loop_header);

        // ----- Exit: store each final sum back to outputs[k] -----
        builder.switch_to_block(loop_exit);
        builder.seal_block(loop_exit);
        for k in 0..n_outputs {
            let off = builder
                .ins()
                .iconst(types::I64, (k as i64) * f64_size);
            let slot = builder.ins().iadd(p_outputs, off);
            // Note: at loop_exit we read header_sums[k] (sealed values).
            builder
                .ins()
                .store(MemFlags::trusted(), header_sums[k], slot, 0);
        }
        builder.ins().return_(&[]);
        builder.finalize();

        // ----- 4. Verify + define + finalize -----
        verify_function(&ctx.func, module.isa())
            .map_err(|e| format!("verify_function: {e}"))?;
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
        let cols: &[usize] = match a {
            AggExpr::SumColumn(c) => std::slice::from_ref(c),
            AggExpr::SumProductColumns(a, b) => {
                if a == b {
                    return Err(format!(
                        "FusedFilterAggSpec: aggregate {i} multiplies a column by itself"
                    ));
                }
                &[*a, *b][..]
            }
        };
        for &c in cols {
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
            let v = builder
                .ins()
                .load(types::F64, MemFlags::trusted(), slot, 0);
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
            let v = builder
                .ins()
                .load(types::I32, MemFlags::trusted(), slot, 0);
            let imm = builder
                .ins()
                .iconst(types::I32, i64::from(clause.imm_i32));
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
    agg: AggExpr,
) -> Value {
    let load_f64 = |b: &mut FunctionBuilder, col: usize| -> Value {
        debug_assert_eq!(col_tys[col], ColumnTy::Float64);
        let elem_size = b.ins().iconst(types::I64, 8);
        let off = b.ins().imul(i, elem_size);
        let slot = b.ins().iadd(col_ptrs[col], off);
        b.ins().load(types::F64, MemFlags::trusted(), slot, 0)
    };
    match agg {
        AggExpr::SumColumn(c) => load_f64(builder, c),
        AggExpr::SumProductColumns(a, b_idx) => {
            let av = load_f64(builder, a);
            let bv = load_f64(builder, b_idx);
            builder.ins().fmul(av, bv)
        }
    }
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
            jit.run(
                shipdate.len() as i64,
                inputs.as_ptr(),
                outputs.as_mut_ptr(),
            );
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
            aggregates: vec![
                AggExpr::SumColumn(0),
                AggExpr::SumProductColumns(0, 1),
            ],
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
        };
        match FusedFilterAggJit::try_build(&spec) {
            Err(e) => assert!(e.contains("incompatible"), "got: {e}"),
            Ok(_) => panic!("expected validation to reject type mismatch"),
        }
    }
}
