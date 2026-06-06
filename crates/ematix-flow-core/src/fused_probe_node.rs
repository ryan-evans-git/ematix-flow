//! PV.3b — the fused-probe logical node + its physical planner.
//!
//! The push-fusion recognizer ([`crate::push_fusion_rule`]) rewrites a star's
//! fact-probing dimension groups into a single [`FusedProbeNode`]: a custom
//! logical node the [`FusedProbePlanner`] turns into an
//! [`EmatPushPipelineExec`] — ONE fact pass that probes every pre-reduced
//! dimension (membership or i64-payload) instead of a take-gather per join.
//!
//! Mechanism (b) (architect): an explicit `UserDefinedLogicalNode` + an
//! `ExtensionPlanner`, so there is NO fragile physical-plan re-detection — the
//! node names exactly what to fuse and the planner resolves columns by name
//! against the planned children. Gated `EMAT_PUSH_PIPELINE=1`, default OFF.
//!
//! ## Trait surface note
//! `UserDefinedLogicalNodeCore: Eq + PartialOrd + Hash`. `DFSchema` has no
//! `PartialOrd`, so the comparison traits are hand-implemented over the
//! SEMANTIC fields (`inputs`/`builds`/`emit`) and skip `schema` — which is a
//! pure function of them, so excluding it preserves the equivalence.

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::{DFSchemaRef, DataFusionError, Result};
use datafusion::execution::session_state::SessionState;
use datafusion::logical_expr::{
    Expr, LogicalPlan, UserDefinedLogicalNode, UserDefinedLogicalNodeCore,
};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::physical_planner::{ExtensionPlanner, PhysicalPlanner};

use crate::emat_push_pipeline_exec::{BuildBinding, BuildSource, EmatPushPipelineExec, EmitCol};

/// One dimension reduction in a [`FusedProbeNode`], resolved by COLUMN NAME
/// (the planner maps names → indices against the planned child schemas, so the
/// node is robust to whatever projection/partition mode the physical planner
/// assigns the children).
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub struct BuildSpec {
    /// Fact (probe) FK column name, e.g. `l_partkey`.
    pub probe_fk: String,
    /// Dim key column name in the build subquery output, e.g. `p_partkey`.
    pub build_key: String,
    /// Payload column name in the build output: `Some` → Inner i64-payload
    /// probe; `None` → LeftSemi membership.
    pub payload: Option<String>,
    /// Require the build key unique (collapsing an INNER join → `true`).
    pub require_unique: bool,
}

/// How one [`FusedProbeNode`] output column is produced, by NAME.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Hash)]
pub enum EmitSpec {
    /// Pass a probe (fact) column through, by name (Int64/Float64).
    ProbeColumn(String),
    /// `price * (1 - disc)` over two probe f64 columns, by name → Float64.
    ProbeRevenue { price: String, disc: String },
    /// Carry build `build_idx`'s recovered i64 payload (emitted Int64).
    BuildPayload { build_idx: usize },
}

/// The fused-probe logical node. Inputs are `[probe, build_0, …, build_{n-1}]`
/// (probe = the fact scan; one build per [`BuildSpec`], same order). Output
/// schema is the operator's emit schema (payloads emitted as Int64).
#[derive(Debug, Clone)]
pub struct FusedProbeNode {
    /// `[probe, build_0, …, build_{n-1}]`.
    pub inputs: Vec<LogicalPlan>,
    /// One per build (aligned with `inputs[1..]`).
    pub builds: Vec<BuildSpec>,
    /// One per output column.
    pub emit: Vec<EmitSpec>,
    /// Output schema (`l_suppkey:Int64, volume:Float64, o_year:Int64` for Q08).
    pub schema: DFSchemaRef,
}

// --- comparison traits over the semantic fields only (schema is derived) ---

impl PartialEq for FusedProbeNode {
    fn eq(&self, o: &Self) -> bool {
        self.inputs == o.inputs && self.builds == o.builds && self.emit == o.emit
    }
}
impl Eq for FusedProbeNode {}
impl PartialOrd for FusedProbeNode {
    fn partial_cmp(&self, o: &Self) -> Option<Ordering> {
        (&self.inputs, &self.builds, &self.emit).partial_cmp(&(&o.inputs, &o.builds, &o.emit))
    }
}
impl Hash for FusedProbeNode {
    fn hash<H: Hasher>(&self, h: &mut H) {
        self.inputs.hash(h);
        self.builds.hash(h);
        self.emit.hash(h);
    }
}

impl UserDefinedLogicalNodeCore for FusedProbeNode {
    fn name(&self) -> &str {
        "FusedProbe"
    }

    fn inputs(&self) -> Vec<&LogicalPlan> {
        self.inputs.iter().collect()
    }

    fn schema(&self) -> &DFSchemaRef {
        &self.schema
    }

    fn expressions(&self) -> Vec<Expr> {
        // The emit/payload exprs live inside the build subqueries (already
        // planned children); the node exposes none for the optimizer to
        // rewrite — its column refs are internal.
        vec![]
    }

    fn fmt_for_explain(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let kinds: Vec<&str> = self
            .builds
            .iter()
            .map(|b| {
                if b.payload.is_some() {
                    "payload"
                } else {
                    "member"
                }
            })
            .collect();
        write!(
            f,
            "FusedProbe: builds=[{}], out_cols={}",
            kinds.join(","),
            self.emit.len()
        )
    }

    fn with_exprs_and_inputs(&self, _exprs: Vec<Expr>, inputs: Vec<LogicalPlan>) -> Result<Self> {
        if inputs.len() != self.inputs.len() {
            return Err(DataFusionError::Internal(format!(
                "FusedProbe expects {} inputs, got {}",
                self.inputs.len(),
                inputs.len()
            )));
        }
        Ok(Self {
            inputs,
            builds: self.builds.clone(),
            emit: self.emit.clone(),
            schema: self.schema.clone(),
        })
    }
}

/// Plans a [`FusedProbeNode`] into an [`EmatPushPipelineExec`], resolving every
/// column name to an index against the already-planned physical children.
#[derive(Debug, Default)]
pub struct FusedProbePlanner;

#[async_trait]
impl ExtensionPlanner for FusedProbePlanner {
    async fn plan_extension(
        &self,
        _planner: &dyn PhysicalPlanner,
        node: &dyn UserDefinedLogicalNode,
        _logical_inputs: &[&LogicalPlan],
        physical_inputs: &[Arc<dyn ExecutionPlan>],
        _session_state: &SessionState,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        let Some(node) = node.as_any().downcast_ref::<FusedProbeNode>() else {
            return Ok(None); // not ours — let another planner try
        };
        if physical_inputs.len() != node.inputs.len() || physical_inputs.is_empty() {
            return Err(DataFusionError::Internal(format!(
                "FusedProbe: expected {} physical inputs, got {}",
                node.inputs.len(),
                physical_inputs.len()
            )));
        }

        let probe = physical_inputs[0].clone();
        let probe_schema = probe.schema();
        let resolve = |sch: &SchemaRef, name: &str| -> Result<usize> {
            sch.index_of(name).map_err(|_| {
                DataFusionError::Internal(format!("FusedProbe: column `{name}` not in schema"))
            })
        };

        // builds[i] ↔ physical_inputs[i + 1].
        let mut builds = Vec::with_capacity(node.builds.len());
        for (bi, spec) in node.builds.iter().enumerate() {
            let bplan = physical_inputs[bi + 1].clone();
            let bsch = bplan.schema();
            let key_col = resolve(&bsch, &spec.build_key)?;
            let payload_col = match &spec.payload {
                Some(p) => Some(resolve(&bsch, p)?),
                None => None,
            };
            let probe_fk_col = resolve(&probe_schema, &spec.probe_fk)?;
            builds.push(BuildBinding {
                source: BuildSource::Plan {
                    plan: bplan,
                    key_col,
                    payload_col,
                },
                probe_fk_col,
                require_unique: spec.require_unique,
            });
        }

        let mut emit = Vec::with_capacity(node.emit.len());
        for espec in &node.emit {
            emit.push(match espec {
                EmitSpec::ProbeColumn(name) => EmitCol::ProbeColumn {
                    col: resolve(&probe_schema, name)?,
                },
                EmitSpec::ProbeRevenue { price, disc } => EmitCol::ProbeRevenue {
                    price_col: resolve(&probe_schema, price)?,
                    disc_col: resolve(&probe_schema, disc)?,
                },
                EmitSpec::BuildPayload { build_idx } => EmitCol::BuildPayload {
                    build_idx: *build_idx,
                },
            });
        }

        let out_schema: SchemaRef = Arc::new(node.schema.as_arrow().clone());
        let exec = EmatPushPipelineExec::new(probe, builds, emit, out_schema);
        // PV.4.0: opt into the overlap path (decode the fact concurrently with
        // the dim build) when `EMAT_PV4_OVERLAP=1`; otherwise the serial
        // build-then-probe path. Read once here at planning time, never hot.
        let exec = match crate::emat_push_pipeline_exec::pv4_overlap_from_env() {
            Some(buf) => exec.with_overlap(buf),
            None => exec,
        };
        Ok(Some(Arc::new(exec)))
    }
}
