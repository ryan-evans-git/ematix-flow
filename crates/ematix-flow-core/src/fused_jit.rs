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
}
