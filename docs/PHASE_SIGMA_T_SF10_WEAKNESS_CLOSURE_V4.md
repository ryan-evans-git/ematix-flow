# Σ.T (V4) — pure-OSS strategy, Red Hat-style services revenue

**Status:** strategic plan (board-readable, not a release artifact)
**Date:** 2026-05-25
**Author:** architect agent (cold-read, no main-thread context)
**Branch:** `perf/sigma-q-single-node-parity`
**Predecessors:**
- V1 (`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE.md`) — 2-engineer, codegen-tax-constrained, conservative menu.
- V2 (`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V2.md`) — scope unrestricted but engineering-scarce; recommended Moderate cohort.
- V3 (`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V3.md`) — 50 engineers funded; B+D hybrid with a commercial cloud product (Track G) as the revenue layer.

**What V4 is.** V3 picked a B+D hybrid with **ematix Cloud** — a multi-tenant SaaS layer on top of the OSS engine — as the revenue engine. V4 **deletes Track G**. ematix-flow stays pure OSS under Apache 2.0. Anyone can self-host. Anyone (including AWS / GCP / Azure) can ship a managed service of it. ematix Inc. never operates compute on behalf of customers and never sells a SaaS markup. Revenue comes from **paid help running the OSS**: 24/7 support contracts, consulting, custom feature development, training, certification, and compliance attestation. The model is **Red Hat pre-IBM applied to data infrastructure**.

**V4's central question:** can ematix-flow ship the V2 Ambitious cohort outcomes (SF=10 22q geomean 0.45–0.55, SF=100 cluster bench published, learning optimizer in production, Iceberg+sidecar storage layer) on a Red Hat-style services revenue model at 30–80 employees?

**The one-sentence answer:** **Yes, conditionally — but only with a reduced engineering scope, a longer calendar, and explicit acceptance that the company's revenue ceiling is ~$50–150M ARR (not Snowflake-scale).** Specifically: 30–40 engineers + 20–40 customer-facing employees can sustain a Red Hat-style services business funded by $10–20M Series A and ~$15–25M ARR by Year 3, reaching profitability by Year 4. The V2 Ambitious technical agenda is **deliverable in 24–30 months instead of 18**, because the field-engineering and support load consume ~25% of effective engineering capacity that V3 charged to a dedicated cloud team. The SF=100 cluster bench and the open Iceberg execution layer remain achievable. The learning optimizer remains achievable. **What is not achievable on this model is the cloud-scale revenue path that would fund a $50–80M Series B at $250–400M valuation.** If the project's owners require that funding shape, V3's commercial cloud is the right answer; V4 is not.

---

## Table of contents

- [Part 1 — Repositioning without a cloud product](#part-1--repositioning-without-a-cloud-product)
- [Part 2 — Revenue model in detail](#part-2--revenue-model-in-detail)
- [Part 3 — Team structure at 30–80 employees](#part-3--team-structure-at-3080-employees)
- [Part 4 — Capital and runway](#part-4--capital-and-runway)
- [Part 5 — Competitive deep-dive (services-business framing)](#part-5--competitive-deep-dive-services-business-framing)
- [Part 6 — Year-by-year milestones (V4)](#part-6--year-by-year-milestones-v4)
- [Part 7 — Risks and pre-mortem (V4-specific)](#part-7--risks-and-pre-mortem-v4-specific)
- [Part 8 — Recommendation](#part-8--recommendation)

---

## Part 1 — Repositioning without a cloud product

### 1.1 The wedge story V3 used and which parts survive

V3's four wedge claims, each rated for V4 viability:

| V3 wedge claim | V4 status | Why |
|---|---|---|
| 1. **Open Iceberg execution layer** ("Tabular acquired the spec; we own the canonical execution engine") | **Survives unchanged.** | The moat is an open-source position; it does not depend on selling a cloud. If anything, V4 strengthens this claim — we cannot be accused of locking customers into our cloud because we don't have one. |
| 2. **Cluster-scale that DuckDB / MotherDuck cannot reach** (SF=100, SF=1000) | **Survives unchanged.** | Customers run the cluster themselves on AWS/GCP/their own metal. ematix Inc. helps them operate it. The benchmark is OSS-engine-level, not cloud-product-level. |
| 3. **Engine that learns from every query** (Σ.L production loop) | **Survives but reframed.** | V3 framed this as "ematix Cloud's optimiser gets smarter for you over time." V4 reframes: "Your self-hosted ematix-flow optimiser learns from your workload — ematix Inc. consultants help you read its observations and tune the resulting plans." The wedge is the same; the buyer experience changes from "managed" to "owned + supported." |
| 4. **5–10× cheaper than Snowflake on the same Iceberg tables** | **Survives and strengthens.** | V3's claim required customers to migrate to ematix Cloud to realise the saving. V4's claim is purer: customers run ematix-flow on their own AWS/GCP compute (which they pay AWS/GCP for directly at cost) and pay ematix Inc. for support — total cost is even lower than V3's cloud markup because there is no markup. The "5–10×" multiple may extend to 10–20× for cost-disciplined self-hosters. |

**The V4 positioning sentence:**

> **ematix-flow is the fastest, most adaptive open-source distributed SQL engine. Free forever, Apache 2.0, no commercial tier. ematix Inc. is the company behind it — we help enterprises run it in production.**

This is a **single positioning sentence**, not the eight-claim back-of-the-box from `ematix.dev/concepts/why-ematix-flow.mdx`. The eight claims remain accurate; the box-back is for users. The positioning sentence is for the *commercial* audience: the CIO, VP-Data, Director-Platform-Engineering who is choosing whether to onboard ematix Inc. as a vendor.

### 1.2 New differentiation vs Starburst, Altinity, EnterpriseDB

Every services-business comparable already does support+consulting+training on an OSS engine. V4 ematix Inc. needs differentiation **at the company layer**, not just the engine layer. Five honest differentiation axes:

| Axis | ematix Inc. | Starburst (Trino) | Altinity (ClickHouse) | EnterpriseDB (Postgres) |
|---|---|---|---|---|
| **Engine perf vs alternatives at same workload** | Top tier for general TPC-H + cluster (per V2 Ambitious cohort) | Mid tier (Trino is mature but JVM-paced) | Top tier for time-series / high-cardinality; weak for joins | Top tier for OLTP; not analytical |
| **Engine governance** | Single vendor (us) for now; explicit Apache Foundation track in Year 2 | Single vendor (Starburst leads); Trino community is Apache-licensed but not Foundation-governed | Single vendor (Altinity does not control ClickHouse core; YDB-derived Russia origin creates governance complexity) | Multi-vendor, mature Apache governance |
| **Adaptive / learning optimizer** | **Yes** — Σ.L production loop | No | No | No |
| **Open table-format alignment** | **Iceberg-native** (not Delta, not proprietary, not MergeTree) | Iceberg, Hive, Delta — multi-format federation | ClickHouse `MergeTree` proprietary; weak Iceberg | Iceberg via FDW; not native |
| **Pure-OSS posture (no BUSL/SSPL)** | **Yes, Apache 2.0 forever** | Trino is Apache; Starburst's Galaxy SaaS is proprietary; Starburst Enterprise has proprietary features (Stargate, Insights) | ClickHouse core Apache 2.0; Altinity wraps it | Postgres is BSD; EnterpriseDB has proprietary tooling |
| **Hyperscaler clone protection** | **Explicit "we welcome AWS ematix"** | Same | Same | Same |
| **Cluster-scale at SF≥100 with no master node** | **Yes** — Arrow Flight peer mesh | Yes — Trino has it (coordinator-worker) | Limited (sharded; no symmetric mesh) | No |

**Three differentiators that hold up under scrutiny:**

1. **The learning optimizer.** Σ.L's persistent cross-run observer is genuinely novel in OSS. Starburst, Altinity, EnterpriseDB don't have it. Snowflake has a private equivalent. This is the single sharpest differentiator and the one most worth investing services revenue into proving with customer attestation in Year 1–2.

2. **Iceberg-native posture without a competing format.** Starburst sells Trino-on-Iceberg, but Trino is JVM-paced and Starburst's go-to-market is federated-query — not "Iceberg is the strategic centre." Altinity sells ClickHouse, which doesn't even use Iceberg natively. EnterpriseDB doesn't compete in the analytical-warehouse layer. **ematix-flow is the only commercial-services-backed Rust engine whose strategic centre is Iceberg.** This is a marketing wedge.

3. **Pure-OSS posture, credibly maintained.** Starburst's "Galaxy" SaaS and Stargate features are proprietary — large enterprise procurement teams notice this. EnterpriseDB is closer to pure but has wrapper tooling. ematix Inc.'s explicit Apache 2.0 / no-BUSL / no-SSPL commitment, written into the company charter, lowers procurement friction for risk-averse Fortune 500 buyers who got burned by Elastic 2021 or HashiCorp 2023.

The first differentiator is technical and improves with engine investment. The second is structural and improves with Iceberg community engagement (Track H). The third is governance and improves with explicit posture commitments + an Apache Foundation track (see §1.4).

### 1.3 What ematix Inc. is *not* trying to be

Stated explicitly because services-businesses drift if they don't say no:

- **Not a SaaS company.** No `flow.ematix.com`. No multi-tenant compute. No per-query billing.
- **Not a feature-tier company.** No "ematix Pro." No "Enterprise Edition" with hidden features.
- **Not a BUSL company.** Apache 2.0 forever. Written into the company charter, not just the README.
- **Not a "managed private cloud" company at scale.** §8 mentions a managed-in-customer-account tier as a possible add-on, but the company's centre of gravity is consulting + support, not running compute.
- **Not a category-redefining startup.** This is a Red Hat-pattern infrastructure-services company. The exit math is profitable bootstrap or strategic acquisition at $300M–$1B, not Snowflake-IPO.

### 1.4 Apache Foundation / CNCF route — pursue in parallel

The Red Hat model is *enabled* by being a foundation-governed OSS project (Linux kernel, glibc, Apache HTTPD, OpenShift's upstream OKD). The foundation gives the OSS an existence that survives the company. ematix Inc.'s services credibility benefits from a project that obviously isn't going to BUSL-trap customers — because the project literally cannot, once foundation-governed.

**Recommendation: pursue both tracks deliberately.**

- **CNCF Sandbox → Incubating → Graduated.** Standard 3–5 year path. Sandbox proposal in Year 1; Incubating in Year 2; Graduated in Year 3–4. CNCF fits because ematix-flow is cloud-native (distributed, Kubernetes-friendly via `k8s://service.namespace:port` peer discovery — see `why-ematix-flow.mdx` §5) but not Java-centric.
- **Apache Software Foundation.** Alternative or supplementary. ASF is the precedent for many data-infrastructure projects (Arrow, DataFusion, Iceberg, Spark, Pinot). ematix-flow's tight DataFusion + Iceberg integration argues for ASF specifically. ASF is harder for a single-vendor-dominated project to enter — they require diversity of committers from independent companies — so the Year 1–2 push is to seed independent committers.

The two tracks are not mutually exclusive: Arrow is ASF-incubated *and* CNCF-adjacent. Pick the track in Year 2 based on which community engagement has produced more independent committers.

**Cost of pursuing foundation track:** ~20% of one engineer's time in Year 1 (sandbox proposal, governance documentation, RFC process); ~50% of Track H DevRel lead's time in Year 2 (community engineering, committer mentoring, license compliance audit). **Not free, but small relative to the strategic upside.**

---

## Part 2 — Revenue model in detail

### 2.1 Revenue lines and Year-3 targets

| Line | Target ARR / customer | Customers Y1 | Customers Y3 | Y3 ARR (low) | Y3 ARR (high) | Margin |
|---|---:|---:|---:|---:|---:|---|
| **A. Enterprise Support (24/7, named TAM)** | $60K–$300K | 5–10 | 40–80 | $4M | $20M | 70–80% |
| **B. Consulting (custom dev, migrations, performance engagements)** | $150K–$800K / project | 3–8 projects | 15–35 projects/yr | $4M | $15M | 30–50% |
| **C. Training & Certification** | $3K–$15K per student | 0 | 800–3000 students/yr | $3M | $15M | 60–80% |
| **D. Managed-in-customer-account (BYOC)** | $120K–$500K | 0 | 10–25 | $2M | $8M | 50–70% |
| **E. Compliance attestation (SOC2 / HIPAA / PCI templates + audits)** | $40K–$150K | 0 | 15–30 | $1M | $3M | 50–70% |
| **F. OEM / embedded licensing** (ematix-parquet, planner crate) | $50K–$300K | 0 | 5–15 | $0.5M | $3M | 90%+ |
| **G. Sponsorships (hyperscaler-paid, foundation-mediated)** | $250K–$1M | 0 | 2–4 | $0.5M | $3M | 100% |
| **Total** | | **5–10** | **80–180** | **$15M** | **$67M** |  |

**Realistic Year-3 ARR: $20–30M**, biased to the lower-middle of the table. Honest about: most service-business comparables underperform their initial projections by 30–50% in Year 3. The Y3 low column ($15M) is the actual planning baseline; the Y3 high column ($67M) is the stretch.

**Comparables anchor.** The table is calibrated against:

- **EnterpriseDB:** ~$200M revenue, ~700 employees, ~25 years old. Revenue/employee ~$285K. ematix Inc. at Y3 targeting $20–30M / 80 employees = ~$250–375K/employee — comparable.
- **Acquia:** ~$200M ARR on Drupal services, ~600 employees, ~17 years old. Similar ratio.
- **Altinity:** ~$30–50M ARR (estimated, private) on ClickHouse services, ~120 employees, ~9 years old. Revenue/employee ~$250–400K. ematix Inc. tracks closest to this comparable.
- **Crunchy Data:** ~$30M ARR (estimated) on Postgres services, ~100 employees, ~13 years old. Similar.
- **Red Hat early years:** ~$10M revenue at year 4 (1997), ~$80M by IPO 1999. ematix Inc. on a *Red Hat-early-years* trajectory hits $30–50M ARR by Y5–Y6, not Y3. **The Y3 $20–30M target is more ambitious than Red Hat's actual trajectory at the same age.** Worth flagging.

The honest assessment: **Y3 $20–30M ARR is achievable but requires Year-1 design partners and disciplined sales execution from M9 onwards.** It is not a default outcome. Y3 below $10M is a real downside scenario (see Part 7).

### 2.2 Line-by-line detail

#### Line A — Enterprise Support

**Product.** 24/7 support contract with named TAM (Technical Account Manager), engineer-level escalation, SLA on response time (1 hour P0, 4 hour P1, business-day P2-3), version-specific support windows for OSS releases, security advisory pre-release access, RFC participation for influence on roadmap. Three tiers:

| Tier | Price/yr | What's included |
|---|---:|---|
| **Standard** | $60K | Business-hours support, P1–P3 SLA, quarterly review, monthly newsletter, public-bug-tracker priority |
| **Production** | $150K | 24/7 support, P0–P3 SLA, named TAM, monthly review, version-pinned hotfix backports |
| **Strategic** | $300K | Production + custom feature votes, security advisory pre-release (30-day window), on-site visits (2/year), RFC sponsorship |

**Buyer.** Director of Platform Engineering or VP Data at a company running ematix-flow in production on workloads they cannot afford to have unavailable. Decision-maker is typically not the CFO; it's the engineering org choosing operational insurance.

**Sales motion.** Inbound from OSS adopters who hit the operational threshold ("we have 5+ engineers using this; we need a vendor relationship"). Sales cycle 3–9 months. Standard model — closely tracks Starburst Enterprise / Altinity Stable Builds / EnterpriseDB Standard.

**Hire requirements.** Customer Success Manager + Support Engineer + (rotating) on-call IC engineer. Estimated 1 CS + 0.5 support engineer + 0.25 IC per 8–12 customers.

**Risk.** Hyperscaler-clone risk hits here hardest. If AWS ships Amazon ematix, the operational-insurance buyer asks "why pay you when AWS will support it under their managed service?" Mitigation: **we are the deepest experts**, can reach into engine internals AWS support cannot, and have direct relationships with maintainers. Red Hat thrived for 20 years with the same dynamic vs cloud Linux.

#### Line B — Consulting

**Product.** Engagement-based custom work: migrations from Snowflake/Databricks/Redshift to ematix-flow; performance engagements (close the last 30% gap on customer-specific workloads); custom connector/operator development; cluster-scale-out engagements; PoC support for evaluator accounts.

**Buyer.** Engineering Manager or Director with budget authority for project work, $50K–$500K range.

**Sales motion.** Often follows a Support contract (customer hits a use case Support doesn't cover). 1–4 month engagement, fixed-price or T&M. Margin lower than Support (30–50% vs 70–80%) because it's labour-intensive.

**Hire requirements.** Field Engineers / Solutions Architects who are 70% IC engineers, 30% customer-facing. Estimated 1 FE per 3–5 active engagements.

**Strategic value.** This is where ematix Inc. learns what customers actually use the engine for, which feeds the engineering roadmap. **Field Engineering is the company's customer-empathy organ; underinvesting here means Engineering builds the wrong things.**

#### Line C — Training & Certification

**Product.** Three layers:

1. **Free tier.** ematix.dev/learn — onboarding tutorial, conceptual docs, video walkthroughs. Free forever. Lead-gen and adoption funnel.
2. **Paid course tier.** $300–$2000/course online (self-paced or instructor-led cohort). Targeted at data engineers and platform engineers ramping into ematix-flow.
3. **Certification.** "ematix-flow Certified Administrator" ($300 exam fee) and "ematix-flow Certified Developer" ($300). Plus "ematix-flow Certified Cluster Operator" ($500) for the cluster-scale persona.

**Buyer.** Individual engineers (paying themselves or expensing) for certifications; employers for cohort training.

**Sales motion.** Self-serve online for most volume; B2B sales for corporate cohort training ($10K–$50K per cohort).

**Hire requirements.** Curriculum lead + instructor pool (3–5). Year 1: 1 hire. Year 2: 3–5.

**Strategic value.** Two layers. (1) Revenue with high margin (60–80%). (2) **Channel multiplier** — every certified engineer is a potential future Support / Consulting customer relationship lead inside their company. Red Hat Certified Engineer (RHCE) program is the precedent: 30,000+ RHCEs created the install-base for RHEL Enterprise Support sales. ematix Inc. needs the same flywheel.

**Caveat.** Training revenue scales with student count, and student count scales with OSS adoption. Year 1 with <2000 GitHub stars: training revenue is near zero. Year 3 with 10,000+ stars: training revenue can hit $5–15M.

#### Line D — Managed-in-customer-account (BYOC)

**Product.** ematix Inc. operates ematix-flow inside the customer's AWS / GCP / Azure account. Customer pays cloud provider directly for compute. ematix Inc. charges a flat monthly operations fee (not a per-query markup). Includes: ops automation (deployment, upgrades, monitoring), 24/7 on-call (escalates to Production Support tier), capacity planning, security patching.

**Why this isn't a SaaS markup.** Customer's cloud bill goes to AWS at their negotiated rate, not to ematix Inc. ematix Inc. is paid for operations labour, not compute markup. The economic relationship is identical to a system-integrator running customer infrastructure.

**Buyer.** Mid-market data team that wants ematix-flow but doesn't want to staff its own cluster ops. Typical company: 50–500 engineers, $10M–$1B revenue, data-team-of-5 that needs production scale.

**Sales motion.** 6–12 month cycle. Often follows a Consulting migration engagement (we migrated you in; we're now running it for you).

**Hire requirements.** SRE / cluster operations team. Estimated 1 SRE per 5–10 BYOC customers (Year 1–2) → 1 SRE per 15–25 (Year 3 with operational automation).

**Strategic value.** This is the **boundary case** with V3's cloud product. The honest difference: V3's ematix Cloud is multi-tenant, ematix Inc. operates the compute, customer pays per-query markup. V4 BYOC is single-tenant in the customer's account, customer pays compute to AWS at cost, ematix Inc. is paid for operations labour. The economic outcome for ematix Inc. is **dramatically lower revenue per customer** ($120K–$500K vs $200K–$2M for V3 cloud) but **dramatically lower capital intensity** (no multi-tenant control plane, no billing infrastructure, no noisy-neighbour ops). For a 30–80-employee company, BYOC is the appropriate scale.

**Risk.** Operations labour does not scale gracefully. At 50 BYOC customers, the SRE team is 5–10 people; at 200 customers, 20–40. This line **caps** at the SRE-team-size ematix Inc. can sustain. The cap is around 30–50 customers; revenue cap around $5–15M ARR for this line alone.

#### Line E — Compliance attestation

**Product.** SOC 2 Type II, HIPAA, PCI DSS, ISO 27001, FedRAMP attestation packages for customers running ematix-flow in regulated environments. Includes: pre-built control matrices, audit-ready evidence collection scripts, expert review of customer's specific deployment, liaison with auditing firms. **Not the audit itself** (the customer pays their auditor); the package that gets them audit-ready.

**Buyer.** GRC (Governance / Risk / Compliance) lead or VP Engineering at regulated companies (finance, health, government).

**Sales motion.** Often bundled with Production Support tier. Project-style fixed-fee.

**Hire requirements.** 1 compliance specialist (Year 2). 2–3 by Year 3.

**Strategic value.** Unlocks regulated-industry adoption that would otherwise stall on procurement. Modest revenue but high-leverage for customer-base expansion.

#### Line F — OEM / embedded licensing

**Product.** Commercial license for ISVs / cloud vendors who want to embed ematix-flow components (ematix-parquet, ematix-flow-planner, the distributed shuffle) without the Apache 2.0 attribution overhead, or with custom indemnification.

**Counterintuitive note.** Apache 2.0 *permits* embedding without separate licensing. OEM licensing is mostly for: (a) indemnification — large enterprises want a contractual party for IP liability; (b) custom support tiers for vendor partners; (c) early-access to non-public features. The "licensing" framing is closer to "premium support for embedders" than pure license-fee revenue.

**Buyer.** ISV CTO / VP-Eng or cloud vendor product team. E.g., a BI vendor wanting to embed our parquet decoder; a cloud vendor wanting indemnification before shipping a managed service.

**Risk.** Small line, doesn't scale, but high-margin. Don't over-prioritise.

#### Line G — Sponsorships

**Product.** Hyperscaler / large enterprise pays ematix Inc. (or via foundation, see §1.4) for a specific roadmap item, security audit, or sustained engineering investment. Examples: AWS sponsors "production-quality FedRAMP attestation in 6 months" for $500K; a fintech sponsors "deterministic-output mode for audit" for $300K.

**Buyer.** Strategic partner with budget to influence roadmap.

**Sales motion.** Director-to-Director relationship; 6+ month cycle.

**Strategic value.** **Highest-margin line** (100% after engineering allocation). Also **highest-leverage** for engineering: sponsorships effectively fund features that would otherwise compete with services delivery for engineering time. The Linux Foundation runs on this pattern at scale; CNCF Sandbox / Incubating status enables it.

### 2.3 Revenue mix shape

Healthy services-business revenue mix at Year 3:

| Line | Y3 ARR (mid) | % of total |
|---|---:|---:|
| A. Enterprise Support | $10M | 40% |
| B. Consulting | $7M | 28% |
| C. Training & Certification | $5M | 20% |
| D. BYOC | $2M | 8% |
| E. Compliance | $0.5M | 2% |
| F. OEM | $0.3M | 1% |
| G. Sponsorships | $0.2M | 1% |
| **Total** | **$25M** | **100%** |

Support is the **anchor product** — recurring, high margin, predictable. Consulting is the **engagement product** — lumpy, lower margin, customer-relationship-building. Training is the **scale-out product** — high margin once content built. BYOC is the **growth optionality** — caps the company but unlocks customers who'd otherwise leave for Snowflake. Compliance, OEM, and Sponsorships are tactical lines.

**At Year 5 (steady state), Support should be 40–50% of revenue, Consulting 20–30%, Training 15–25%.** That's the EnterpriseDB / Acquia / Altinity shape.

### 2.4 Customer count + ARR-per-customer math

| Year | Avg ARR/customer | Customers | ARR |
|---:|---:|---:|---:|
| Y1 (M0–M12) | $100K | 5–15 | $0.5–1.5M |
| Y2 (M13–M24) | $140K | 30–60 | $4–9M |
| Y3 (M25–M36) | $160K | 100–180 | $15–30M |
| Y4 (M37–M48) | $190K | 180–300 | $35–55M |
| Y5 (M49–M60) | $220K | 250–400 | $55–90M |

**Net retention assumption: 110–125%** (standard for services-businesses with multi-line expansion: Support → Consulting → BYOC → expanded Support tier). **Logo churn assumption: 8–15%/year** (services-business norm).

**The Year 5 $55–90M ARR projection is where the model gets honestly interesting.** It's not Snowflake-scale, but it supports a profitable 100–150-employee company, modest IPO, or strategic acquisition at $500M–$1.5B (revenue multiples for services businesses are 4–8×; for SaaS 10–25×).

---

## Part 3 — Team structure at 30–80 employees

### 3.1 Org chart at full scale (Year 3)

```
CEO (founder)
├── VP Engineering ──── manages 4 Directors + Infra/SRE Director
│       │
│       ├── Director, Engine (Tracks A + B + I — eng-core, storage, infra)
│       │   ├── EM, Engine Core (Track A, 8 ICs)
│       │   ├── EM, Storage (Track B, 4 ICs)
│       │   └── EM, Infra / SRE for company internal systems (Track I, 3 ICs)
│       │
│       ├── Director, Distributed & Adaptive (Tracks C + D)
│       │   ├── EM, Distributed (Track C, 5 ICs)
│       │   └── EM, Adaptive Runtime (Track D, 3 ICs)
│       │
│       └── Director, Product Engineering (Tracks E + F)
│           ├── EM, DX (Track F, 4 ICs)
│           └── EM, GPU + R&D (Track E, 2 ICs)
│
├── VP Field Operations ──── owns customer-facing engineering + customer success
│       │
│       ├── Director, Field Engineering (Solutions Architects, Consulting)
│       │   └── Field Engineers (12 ICs at Y3 — see §3.2)
│       │
│       ├── Director, Customer Success (Support + TAM)
│       │   └── CSMs / TAMs (5 at Y3) + Support Engineers (4 at Y3)
│       │
│       └── Director, BYOC Operations (SRE for customer-account managed deployments)
│           └── SREs (4 at Y3)
│
├── VP Training & Certification ──── owns the learning + cert programs
│       └── Curriculum Lead, Instructors (5 at Y3), Cert platform engineer (1)
│
├── Director, DevRel & Community ──── reports to VP Engineering Y1, CEO from Y2
│       └── DevRel + community managers (Track H, 3 ICs at Y3)
│
├── VP Sales (M12 hire) ──── owns commercial GTM for Support + Consulting + BYOC
│       └── Account Executives (3 at Y3), Sales Engineers (2 at Y3 — overlaps Field Eng)
│
├── VP Marketing (M15 hire) ──── content, brand, conferences, ematix.dev
│       └── Content lead, design, ops (3 at Y3)
│
├── CFO (M12 hire) ──── controller, FP&A
│       └── 2 at Y3
│
├── General Counsel (M18 hire) ──── licensing, contracts, M&A, IP, employment
│
└── Head of People (M9 hire) ──── hiring, culture, comp, mentorship
```

### 3.2 Headcount by function

| Function | Year 1 (M12) | Year 2 (M24) | Year 3 (M36) | Notes |
|---|---:|---:|---:|---|
| **Engineering** (Tracks A/B/C/D/E/F/H/I) | 15 | 25 | 32 | Down from V3's 50 by removing Track G (cloud product, 8–10) plus right-sizing |
| **Field Engineering** (consulting, SAs) | 2 | 6 | 12 | Net-new vs V3 |
| **Customer Success / Support** | 1 | 4 | 9 | Net-new vs V3 |
| **Training / Certification** | 0 | 2 | 6 | Net-new vs V3 |
| **BYOC Operations (SRE)** | 0 | 2 | 4 | Net-new vs V3 |
| **DevRel + Community** | 2 | 3 | 3 | Same as V3 |
| **Sales** | 0 | 3 | 5 | Smaller than V3 (no PLG funnel for cloud) |
| **Marketing** | 1 | 3 | 4 | Same |
| **G&A (Finance, Legal, HR)** | 2 | 4 | 6 | Same |
| **Executive** | 3 (CEO, VP Eng, Head of People) | 6 | 8 | Same |
| **Total** | **26** | **58** | **89** | Reaches ~89 by Y3 |

**Engineering as % of total:**

- V3 Year 1: 60% engineering (50/88).
- V4 Year 3: 36% engineering (32/89).

The difference is **customer-facing roles**: V4 has 36 customer-facing employees (Field Eng + CS + Support + Training + BYOC SRE + Sales) vs V3's 20 (Sales + DevRel only). **Services businesses are customer-facing-heavy by design.** Red Hat at $3B revenue had ~12K employees with roughly 50% customer-facing.

**Why 32 engineers (not 40–50)?**

V3 had 50 engineers split across 8 tracks. V4 removes Track G (8–10 engineers for the cloud product). That leaves 40–42. **But** services businesses fund engineering through services revenue, which has lower ARR/employee than SaaS — so 40 engineers funded at services-revenue rates means a much higher total headcount (because customer-facing roles outnumber engineering). To keep total headcount in the 80–90 band at Year 3, engineering compresses to 30–35.

The trade-off is honest: **V4 ships the V2 Ambitious engineering agenda in 24–30 months instead of V3's 18 months**, because there are fewer engineers. See §3.5.

### 3.3 The first 8 hires after the existing team

V3's first-6-hires list adapted for V4:

1. **VP Engineering** — same as V3. Scaled-from-5-to-50 experience, ex-Databricks / Confluent / Stripe-infra.
2. **Senior database internals engineer** (Track A) — same as V3.
3. **Senior distributed systems engineer** (Track C) — same as V3.
4. **Senior storage / Iceberg engineer** (Track B) — same as V3.
5. **Senior DevRel** (Track H) — same as V3. Critical for OSS growth without a SaaS top-of-funnel.
6. **VP Field Operations** (new in V4 vs V3's "Senior cloud-product engineer"). Look for someone who has built a services org at an OSS company — ex-Starburst-CSE, ex-Confluent-pre-sales-eng, ex-EnterpriseDB-services. **This is the second-most-important non-CEO hire after VP Engineering.** They build the entire customer-facing organ that V3 mostly didn't have.
7. **Head of People** (M6–M9 in V3, M3 in V4). Earlier in V4 because customer-facing roles need more deliberate hiring discipline than engineering.
8. **Curriculum / Training lead** (M9 in V4). Earlier than typical because Training & Certification is a flywheel that takes 18+ months to ramp; starting in M9 has Year-3 revenue arriving on schedule.

**Culture-setting risk in V4 is different from V3.** V3's first 6 are all engineering-tilted (5/6 eng + 1 DevRel). V4's first 8 are engineering-tilted but include 2 customer-facing leaders (VP Field Ops + Training lead). Culture should be visibly bicameral from week 1 — engineering excellence *and* customer empathy. Companies that hire engineering-first and bolt-on customer-facing later (cf. early Snowflake, MongoDB) have a known culture-debt to repay.

### 3.4 Stepped hiring plan

| Phase | HC target | Cumulative | Calendar | Gate |
|---|---:|---:|---|---|
| **Phase 0** (current) | ~5 | 5 | M0 | — |
| **Phase 1** | +10 | 15 | M0–M3 | First 8 hires onboarded; first PR from non-founder shipped |
| **Phase 2** | +12 | 27 | M3–M9 | First 3 paying Support customers; SF=100 bench harness running; OSS GitHub stars > 1.5K |
| **Phase 3** | +20 | 47 | M9–M18 | SF=100 bench *published*; ARR > $2M; first 5 BYOC pilots; OSS GitHub stars > 4K; first CNCF Sandbox / ASF Incubator proposal submitted |
| **Phase 4** | +20 | 67 | M18–M30 | ARR > $10M; Training program live with 100+ paying students; Iceberg moat established; cluster SF=1000 bench published |
| **Phase 5** | +22 | 89 | M30–M36 | ARR > $20M; profitable or near-profitable; CNCF Incubating or ASF Incubator status achieved |

**Gates are real.** Phase 3 in V4 is the equivalent of V3's "Phase 3 + Series B close." If the Phase 2 gates aren't met at M9, Phase 3 hiring pauses. V4's smaller Series A (see §4) reduces the cash buffer, so gate enforcement is more important than in V3.

### 3.5 Cross-track engineering velocity at 32 ICs

V3 sized the V2 Ambitious cohort to ship in 12–18 months with 50 engineers. V4 has 32 engineers in steady state (and only 15–20 in Year 1). **Calendar reality:**

| V2 lever | V3 calendar (50 eng) | V4 calendar (15→32 eng) | Notes |
|---|---|---|---|
| L1 sidecar Phase 1 | M3 | M4–M5 | Shipped early in V4; minimal extra cost |
| L3 PGO | M3 | M4 | Same |
| L8 custom CBO | M9 production | M14–M16 | +5–7 months in V4 |
| L10 dynamic filter propagation | M9 | M14–M16 | Co-ships with L8 |
| L13 custom hash join | M9 | M14–M16 | Co-ships with L8 |
| L14 dict-preserved default | M6 | M10 | +4 months |
| L11 compile-time monomorphisation | M15 | M22–M24 | +7–9 months |
| L12 zero-copy column pipeline | M18 | M28–M30 | +10–12 months |
| L15 storage layer Iceberg + sidecar | M18 | M22–M26 | +4–8 months |
| L17 production learning loop | M18 | M22–M24 | +4–6 months |
| L18 fork decision | M12 | M18 | +6 months — same hard-criteria gate |
| **SF=100 cluster bench published** | M9 | M11–M12 | **+2–3 months only** — Track C funded early and aggressively |
| **SF=1000 cluster bench published** | M18 | M24–M27 | +6–9 months |
| **Engineering reaches V2 Ambitious geomean (0.45–0.55)** | M24 | M30–M33 | +6–9 months |

**Key calendar choices V4 makes:**

1. **Track C (Distributed) is funded aggressively early.** SF=100 published bench is the dominant strategic milestone — it's the proof point that attracts both customers (Support contracts) and community (foundation track committers). V4 budgets ~6 engineers on Track C from M3 onwards, only slightly less than V3. SF=100 publication slips only 2–3 months vs V3.

2. **Track A (Engine core) compresses from 8–10 to 6–8 engineers.** L8 + L13 + L10 ship together at M14–M16 instead of M9. The L18 fork decision moves to M18.

3. **Track B (Storage) stays at 4–5 engineers.** Iceberg read-side ships M6–M8; sidecar Phase 2 M10–M12; write-side + Σ.L.5 M22–M26. The Iceberg moat narrative materialises in M18+ rather than M12+.

4. **Track D (Adaptive) stays at 3 engineers.** Σ.L.2 → L17 wire-up depends on Track A's L8; ships ~2 months after L8 production. L17 production loop at M18–M20 in V4 vs M18 in V3 — **roughly same calendar** because Track D was small in V3 anyway.

5. **Track E (GPU) is reduced to 2 engineers and reframed as "R&D / future work."** No cluster-GPU bench in Year 2 (deferred to Year 3+). Metal prototype only.

6. **Track F (DX) stays at 4 engineers.** Python SDK + Web UI + ematix.dev + docs. Heavier in V4 than V3 proportionally because the OSS adoption funnel matters more without a SaaS top-of-funnel.

7. **Track H (DevRel + Community) stays at 3.** Critical for foundation track + OSS adoption.

8. **No Track G.** The 8–10 engineers V3 spent on the cloud product are entirely repurposed — half to Field Engineering, half to ship the engine tracks at the smaller cadence.

**The honest read on velocity:** V4's engineering ships ~70% of V3's pace. The V2 Ambitious cohort outcomes (geomean 0.45–0.55, full Iceberg moat, learning optimizer in production) land in Year 3 instead of Year 2. The SF=100 published bench — the single most important strategic milestone — lands within 2–3 months of V3.

### 3.6 The customer-facing organ in detail

V4's defining structural difference from V3 is the customer-facing organ. Three sub-orgs:

**Field Engineering** (12 at Y3): IC-engineer-tilted (70% IC work, 30% customer-facing). Each FE owns 2–5 active customer engagements. Engagement types:
- Migration projects (Snowflake → ematix-flow, Trino → ematix-flow).
- Performance engagements ("you're 30% slower than DuckDB on workload X; here's the gap closure").
- Custom feature development (sponsored by a customer who needs a specific connector or operator).
- PoC support for evaluator accounts (pre-Support-contract).

FEs report to Director, Field Engineering, who reports to VP Field Operations. **Critical: FEs spend ≥30% of their time writing code that lands in the public OSS engine** — engagements drive engineering features. Without this, Field Engineering becomes a services silo divorced from the engine roadmap.

**Customer Success / Support** (9 at Y3): TAMs (Technical Account Managers) own customer relationships. Support Engineers handle ticket triage and resolution. Each TAM owns 8–15 enterprise accounts. Support Engineers rotate through P0/P1 on-call (escalating to IC engineering ~weekly).

**BYOC Operations** (4 SREs at Y3): Specialised on running ematix-flow inside customer cloud accounts. Each SRE owns ~5–10 BYOC customers' clusters. They are platform engineers, not consultants — repetitive ops automation, not custom code.

**Training organisation** (6 at Y3, reports to VP T&C): Curriculum lead, instructors, certification platform engineer.

**Sales organisation** (5 at Y3, reports to VP Sales): 3 AEs (Account Executives — relationship and procurement), 2 SEs (Sales Engineers — overlaps Field Engineering for pre-sales).

**Total customer-facing at Y3: 36 employees.** That's 40% of company headcount. Engineering is 36% (32 / 89). G&A + Executive is 24%. This is the canonical services-business shape.

---

## Part 4 — Capital and runway

### 4.1 Burn breakdown at full scale (Year 3)

| Cost category | Annual cost | Notes |
|---|---:|---|
| **Headcount** | $19–24M | 32 eng × $300K + 36 customer-facing × $200K (loaded) + 21 G&A/exec × ~$250K avg |
| **Infrastructure** | $1–2M | Smaller than V3 (no multi-tenant cloud); CI + bench cluster + ematix.dev hosting + BYOC observability |
| **Sales & marketing** | $2–4M | Conferences (re:Invent, KubeCon, Data+AI Summit, P99), content, ads. Smaller than V3 because no PLG funnel investment |
| **G&A** | $1.5–3M | Legal, accounting, insurance, software. Slightly lower than V3 |
| **M&A (acqui-hires)** | $5–15M one-time, spread over Y1–Y2 | Smaller than V3's $25–50M — less capital to deploy |
| **Total operating burn (excl. M&A)** | **$23–33M/year** | |

**Realistic burn at Y3 steady state: $25–28M/year.** This is about 60% of V3's $35–48M.

### 4.2 Implied funding raise

| Round | Amount | Implied valuation | Timing | Comparable |
|---|---:|---:|---|---|
| **Seed (existing or top-up)** | $3–7M | $20–40M | M0 | Standard pre-revenue |
| **Series A** | $10–20M | $40–80M | M6–M12 | EnterpriseDB Series A (2008, ~$10M); Altinity (2017, $4M angel + 2020 $4M seed → bootstrap); Crunchy Data was bootstrap |
| **Series B (if growth justifies)** | $25–50M | $150–350M | M24–M30 | Starburst Series A 2019 ($22M @ $122M); Tabular Series A ($26M @ $200M) |
| **Series C (optional, if not acquired)** | $50–100M | $400M–$800M | M36–M48 | EnterpriseDB Series B (2014, $48M); Starburst Series B (2020, $42M @ $400M) |

**The fundraising shape difference vs V3:**

- V3 Series B: $50–80M at $250–400M.
- V4 Series A: $10–20M at $40–80M.

That's roughly **5× smaller dollar raise at 4× lower valuation**. The dilution math: V3 founder gives up ~20% in Series B; V4 founder gives up ~20–25% in Series A but at much lower valuation, so the *dollar* dilution is smaller. Trade-off: less cash buffer in V4 means more execution discipline required.

**The Series B/C path is conditional in V4.** Services businesses at $10–20M ARR can sometimes raise growth rounds, but investor appetite is structurally weaker than SaaS. Mature services businesses (EnterpriseDB, Crunchy Data, Altinity) often **stop raising after Series A or B** and run cash-flow-positive. V4 should plan to be cash-flow-positive at $20–30M ARR with 80–90 employees, treat Series B+C as optional growth capital, and **not** depend on them for survival.

### 4.3 Three funding scenarios

| Scenario | Path | Y3 outcome | Y5 outcome |
|---|---|---|---|
| **A. Bootstrap (no Series B)** | $15M Series A in M6–M12; cash-flow-positive at M30+; no further dilution | $20M ARR, 80 employees, profitable | $50M ARR, 130 employees, profitable, optional IPO or strategic acquisition |
| **B. Modest growth round** | Series A $15M + Series B $35M @ $250M valuation in M30 | $25M ARR, 90 employees, near-profitable | $60M ARR, 150 employees, profitable, IPO-track or acquisition |
| **C. Foundation + sponsor seeded** | $5–10M from hyperscaler sponsorship + foundation-mediated commitments + small Series A $10M | $15M ARR, 70 employees, profitable | $40M ARR, 110 employees, foundation-led governance, project becomes ASF/CNCF graduate, founders potentially less involved at Y5 |

**Honest assessment.** Scenario A is **the most defensible plan** because it doesn't depend on Series B raise climate. Scenario B is the **upside** if growth metrics support a round. Scenario C is the **values-aligned path** if the founder prioritises foundation governance + community ownership over personal financial outcome — it's the most "Red Hat-original" of the three. Red Hat itself was Scenario A → eventually IPO (1999) → eventually IBM ($34B, 2019).

### 4.4 The runway math

| Scenario | Burn Y1–Y2 avg | Cash from Series A | Months runway |
|---|---:|---:|---:|
| Bootstrap (A) | $12M/year | $15M | 15 mo + revenue |
| With Y2 ARR ramp | $12M/year cost − $3M Y2 ARR = $9M net | $15M | 20 mo |

By M18, expected ARR is $3–5M, gross margin ~60%, so contribution is ~$2–3M / year. Burn net of revenue drops to ~$10M / year. Series A $15M gives **18–24 months runway** which is enough to reach $10M ARR target by M24. **The Series A is sized to bridge to revenue.**

**If revenue ramps slower** (Y2 ARR is $1M instead of $3M), runway tightens to 14–16 months, requiring either bridge round or burn cut. The Phase 3 hiring gate at M18 is the discipline mechanism: if ARR isn't trending to $10M by M24, hold hiring at Phase 3 (~47 people) instead of progressing to Phase 4.

### 4.5 Why the cloud product is the funding fork

To make the cost-vs-funding logic concrete:

| Metric | V3 (with cloud) | V4 (no cloud) |
|---|---:|---:|
| Y3 ARR target | $30–100M (cloud + enterprise) | $20–30M (services only) |
| Y3 burn | $35–48M | $23–33M |
| Y3 cash position | dependent on Series C raise | cash-flow-positive on bootstrap path |
| Series A raise | $10–20M | $10–20M (same) |
| Series B raise (if pursued) | $50–80M @ $250–400M | $25–50M @ $150–350M |
| Series C raise | $100–200M @ $500M–$1B | optional; $50–100M @ $400M–$800M |
| Exit band (acquisition) | $1.5B–$3B (cloud business multiple) | $300M–$1.5B (services multiple) |
| Exit band (IPO) | $3B+ (Confluent-style) | $500M–$1.5B (Red Hat-IPO-1999-equivalent) |

The cloud product is what unlocks the SaaS-style multiples ($1.5–3B acquisition; $3B+ IPO). Removing it sets a structural ceiling on exit value. **For founders or boards who can't accept the $300M–$1.5B exit band as the upper bound, V3 (or hybrid V3/V4) is the right answer.**

V4 is the right answer for founders who:

- Are values-aligned with pure-OSS, foundation-track, no-BUSL governance.
- Are comfortable with smaller absolute exit dollars in exchange for slower dilution and more sustainable burn.
- Prioritise long-term project survival over maximising founder equity.
- Want to operate a profitable company at $50M revenue rather than a venture-pressured one at $100M ARR.

---

## Part 5 — Competitive deep-dive (services-business framing)

V3's competitive deep-dive analysed engine-vs-engine competition (DuckDB, ClickHouse, Snowflake, Databricks, Starburst, Trino, Pinot, Materialize, SingleStore). All of that analysis carries forward for the *engine* layer. V4 adds a layer of analysis: **competitor-as-services-vendor.** This is the layer where ematix Inc. competes directly.

### 5.1 Direct services-business competitors

#### Starburst (closest direct competitor)

**Business model.** Trino OSS + Starburst Enterprise (proprietary features: Stargate, Insights, Mission Control, Ranger integration) + Starburst Galaxy (SaaS). Revenue mix is enterprise license + support + Galaxy SaaS. ~$100M+ ARR (estimated), ~400 employees, valued $3.35B at Series D (2022).

**Where they compete with us.** Trino-on-Iceberg is the closest workload to ematix-flow-on-Iceberg. Their enterprise sales motion is well-established; their consulting partner network (Capgemini, Accenture, Deloitte) is substantial.

**Where we win.** (1) Engine performance — Rust beats JVM-Trino on TPC-H by 3–10× per V2 analysis. (2) Pure-OSS posture — Starburst Enterprise's proprietary features create exactly the kind of procurement friction we avoid. (3) Learning optimizer — Trino has no equivalent. (4) Iceberg-strategic-centre — Trino federates across many sources, ematix-flow centres on Iceberg. For organisations consolidating to Iceberg (the strategic 2026+ trend), our positioning is sharper.

**Where they win.** (1) Brand and enterprise sales muscle — Starburst has been in the Fortune-500 procurement loop for 5 years. (2) Federated query — if the customer wants Trino's "one query across many sources," ematix-flow doesn't directly compete. (3) Galaxy SaaS for customers who explicitly want managed.

**Strategy vs Starburst.** Differentiate on pure-OSS posture + Iceberg-strategic-centre + engine perf. Accept that we don't compete for the "federated query" workload. Target Iceberg-centric customers who are running Starburst Enterprise and frustrated with proprietary features or JVM perf.

#### EnterpriseDB (closest model precedent)

**Business model.** PostgreSQL OSS + EDB Postgres Advanced Server (proprietary fork with Oracle compatibility) + Postgres Enterprise Manager + 24/7 support + consulting. ~$200M revenue, ~700 employees, ~25 years old. Private equity-owned since 2019.

**Where they compete with us.** Not at the engine layer (Postgres ≠ analytical engine). But the **services-business pattern** is the closest precedent. EnterpriseDB serves as our company-model template.

**Strategy vs EnterpriseDB.** No direct competition. Use them as a benchmark for revenue/employee, sales motion structure, and product-line shape.

#### Altinity (ClickHouse services precedent)

**Business model.** ClickHouse OSS + Altinity Stable Builds (LTS releases) + Altinity Cloud (managed SaaS) + 24/7 support + consulting + training. ~$30–50M ARR (estimated), ~120 employees, ~9 years old.

**Where they compete with us.** ClickHouse and ematix-flow both target analytical workloads. ClickHouse excels at time-series + high-cardinality; ematix-flow at joins + general TPC-H + cluster scale. **Some workload overlap, mostly different sweet spots.** Altinity is mainly a ClickHouse-services-business comparable, not a head-to-head competitor at workload level.

**Strategy vs Altinity.** Different workload sweet spots; rarely head-to-head. Use as services-business precedent.

#### Crunchy Data (Postgres services)

**Business model.** PostgreSQL services + Crunchy Postgres for Kubernetes + Crunchy Bridge (managed). $30M+ ARR (estimated), ~100 employees, ~13 years old. Acquired by Snowflake (2024, ~$300M estimated).

**Strategic note.** **Crunchy Data was acquired by Snowflake.** This is direct precedent for the V4 exit path: a Postgres-services-business at modest ARR getting acquired by a hyperscale data company. **Probability of similar acquisition for ematix Inc. at Y4–Y5: meaningful.** Snowflake / Databricks / Cloudera / Red Hat-itself / IBM are all plausible acquirers.

#### Aiven (multi-DB managed services)

**Business model.** Managed services for PostgreSQL, MySQL, Kafka, ClickHouse, M3, etc. ~$100M+ ARR, ~400 employees. Centred on "we manage your open-source databases."

**Where they compete with us.** Aiven *could* add ematix-flow to their managed-services menu. **We should welcome this.** A multi-DB managed-services vendor offering ematix-flow expands our adoption surface without our doing any work. Aiven becomes a *channel*, not a competitor.

**Strategy vs Aiven.** Build a partnership relationship. Aiven manages, ematix Inc. provides upstream support contract. Same model as Aiven's relationships with PostgreSQL or Kafka committers.

#### Percona (MySQL + Postgres services)

**Business model.** MySQL + Postgres + MongoDB OSS services. ~$50M+ revenue, ~300 employees, ~18 years old.

**Strategic note.** Same as EnterpriseDB — services-business precedent, no direct competition at workload layer.

### 5.2 Hyperscaler-clone risk and mitigation

The Apache 2.0 licence allows AWS / GCP / Azure to ship "Amazon ematix" / "GCP ematix" / "Azure ematix" as managed services. **V4 explicitly welcomes this**, on the Red Hat / Confluent-Kafka precedent.

**Why hyperscaler cloning is net-positive for ematix Inc.:**

1. **Adoption expansion.** Hyperscaler-managed services bring ematix-flow to customers who would never self-host. The total addressable customer base grows.
2. **Credibility boost.** "AWS ships a managed service of this engine" is the strongest possible signal of engine production-readiness. It accelerates enterprise procurement.
3. **Sponsorship revenue.** Hyperscalers who ship managed services of OSS engines typically pay the upstream community for security advisories, roadmap input, and committer time. Red Hat received this from AWS/Google for Linux; Confluent receives it from AWS for Kafka.
4. **Channel for support sales.** Customers on hyperscaler-managed ematix often hit the threshold where they need expert support that the hyperscaler can't provide at the engine-internals level. They become ematix Inc. Support customers. (Confluent does ~$50M+ ARR from customers running Kafka on AWS MSK who need expert Confluent support.)

**Where hyperscaler cloning is risk:**

1. **BYOC line erosion.** If AWS ships a managed ematix-flow, ematix Inc.'s BYOC line (§2 Line D) competes directly. Mitigation: BYOC is the smallest revenue line (~$2M of $25M Y3); not central.
2. **Training market dilution.** AWS / GCP might ship competing certifications. Mitigation: our certs are engine-internals-focused; theirs are "how to use our managed service." Different markets.

**Net assessment.** Hyperscaler cloning is **strongly net-positive** for ematix Inc. We should actively encourage it — DevRel engages AWS / GCP / Azure database-services teams in Year 1–2, makes integration easy, offers reference support contracts.

### 5.3 Open-source projects we coexist with

Three OSS projects ematix-flow depends on or coexists with, each with strategic implications:

**DataFusion (Apache).** Our planner + execution surface lives on DataFusion. The L18 fork decision (V2 §2.3) is whether to maintain our own fork or stay on upstream. V4 keeps the M18 fork decision; community engagement with DataFusion (Track A + Track H contribute back) is non-negotiable regardless. Foundation track depends on visible upstream good citizenship.

**Apache Iceberg.** The table-format moat is built on Iceberg. We are committed to upstream good citizenship: contribute the sidecar spec (Σ.L.5 / `attach_extension` work), participate in Iceberg PRC, sponsor Iceberg committers via Track H. **Without strong Iceberg community alignment, the entire Iceberg moat narrative is fragile.**

**Apache Arrow.** Arrow is our in-memory data model. Stable, foundation-governed, multi-vendor. Low strategic risk; standard upstream contribution discipline applies.

### 5.4 The wedge map (V4)

Updated from V3 §5.2 for services-business framing:

```
                       What we OWN                         What we DON'T OWN
                       ┌─────────────────────────┐         ┌─────────────────────────┐
What we DO BETTER     │  Engine perf (Rust vs    │         │  Snowflake/Databricks   │
(perf, scale, ops)    │  JVM, TPC-H + cluster)  │         │  cloud-ops maturity     │
                      │                          │         │  (we don't enter this    │
                      │  Learning optimizer     │         │  market in V4)           │
                      │  (Σ.L production loop)  │         │                          │
                      │                          │         │  ManageD private cloud   │
                      │  Iceberg-strategic-     │         │  (Aiven et al own; we    │
                      │  centre engine          │         │  partner)                │
                      │                          │         │                          │
                      │  Pure-Apache-2.0,        │         │  Federated query        │
                      │  no BUSL governance     │         │  ("query 30 sources at   │
                      │  posture                 │         │  once" — Starburst lane) │
                      │                          │         │                          │
                      │  ematix-parquet codec   │         │                          │
                      │  (sibling crate, others │         │                          │
                      │  adopt)                  │         │                          │
                      └─────────────────────────┘         └─────────────────────────┘
                       ┌─────────────────────────┐         ┌─────────────────────────┐
What we DO COMPARABLY  │  SF=10 single-node      │         │  Spark/MLlib ecosystem  │
(parity, eventually)  │  TPC-H vs DuckDB (Y2-3  │         │  (Databricks owns)       │
                      │  target: parity)         │         │                          │
                      │                          │         │  Embedded SDK ergonomics │
                      │  Standard SQL surface   │         │  (DuckDB / MotherDuck)   │
                      │                          │         │                          │
                      │  Conferences, content,  │         │  Proprietary cloud DB    │
                      │  community size         │         │  (Snowflake)             │
                      └─────────────────────────┘         └─────────────────────────┘
```

The top-left box has *more entries* in V4 than V3 because pure-OSS posture and ematix-parquet as a sibling crate become explicit competitive assets. The top-right box is *narrower* because we're not entering the cloud-ops or PLG-funnel markets at all.

---

## Part 6 — Year-by-year milestones (V4)

### Year 1 (M0–M12): proof + first customers

**Team scaling.** 5 → 15 → 27. Phase 0 → 1 → 2.

**Engineering (Tracks A/B/C/D/F/H):**
- M3: V2 Phase T1 outcomes shipped (L1 sidecar Phase 1, L3 PGO, L6). 22q SF=10 geomean ~0.72.
- M6: Sidecar Phase 2 in production. Iceberg read-side adopted. SF=100 bench harness running. ematix.dev relaunch.
- M9: First L13 kernel + L8 design + L14 default flip on opt-in basis. 22q SF=10 geomean ~0.65–0.68.
- M11–M12: **SF=100 published bench** — the dominant strategic milestone. Cluster runs at 15–50× DuckDB single-node, competitive with Trino on the same hardware. Published as blog post + arXiv-style paper + conference talk.

**Customer-facing (Field Ops):**
- M3: VP Field Operations onboarded. First Field Engineer hired.
- M6: First 3 paying Support customers ($60–150K each). First Consulting engagement signed.
- M9: 5–10 paying Support customers. First 2 BYOC pilots (unpaid).
- M12: **Year-1 ARR target $0.8–1.5M.**

**Community / Foundation:**
- M3: CNCF Sandbox proposal drafted (Track H lead).
- M9: CNCF Sandbox proposal submitted (after SF=100 publication strengthens the project's credibility signals).
- M12: GitHub stars > 2K. First Apache Iceberg PR from ematix Inc. team accepted.

**Funding:**
- M0: Seed top-up if needed ($3–7M).
- M6: Series A prep begins.
- M9–M12: **Series A close** ($10–20M @ $40–80M valuation), gated on SF=100 publication + 5+ paying customers.

**Year-1 board read:** "Shipped SF=100 published bench (the strategic proof). 10 Support customers. ARR ~$1M. Engineering pace tracking 70% of V3-equivalent due to smaller team. Foundation track initiated. Series A closed. On track for Y2."

### Year 2 (M13–M24): scale + commercial maturity

**Team scaling.** 27 → 47. Phase 3.

**Engineering:**
- M14–M16: L8 CBO + L13 custom hash join + L10 dynamic filter propagation in production. 22q SF=10 geomean ~0.55–0.58. **All 6 V2-cited losses flipped to wins.** (V2 Moderate cohort outcomes, ~5–7 months later than V3.)
- M18: **L18 fork decision made.** Same explicit hurdle criteria as V3. If GO, fork begins with 6-month MVP gate. If NO-GO, stay on DataFusion upstream.
- M20–M22: Σ.L.5 write-side + L17 production learning loop. Customer attestation: "our workload's optimiser learned 25% improvement over 3 months."
- M22–M24: Iceberg write-side in production. Iceberg `attach_extension` proposal to Apache Iceberg PRC.

**Customer-facing:**
- M15: 20 Support customers. 8 active Consulting engagements. 5 BYOC pilots (3 paid).
- M18: Training program v1 launches (3 self-paced courses + 1 cohort cert). 50–100 students in first quarter.
- M21: 35 Support customers. First Compliance attestation engagement.
- M24: **Year-2 ARR target $5–10M.**

**Community / Foundation:**
- M15: CNCF Sandbox accepted (target). First independent committer.
- M18: SF=1000 bench prep underway.
- M21: GitHub stars > 5K. 5+ independent committers.
- M24: CNCF Incubating proposal submitted.

**Funding:**
- M18–M24: Series B conversation if growth trajectory supports. **Optional.** If Y2 ARR ramp matches plan, Series B can be $25–50M @ $150–350M for accelerated Y3 hiring. If ARR underperforms, stay bootstrap and slow Phase 4.

**Year-2 board read:** "L8/L10/L13 shipped. SF=10 22q geomean at V2-Moderate-cohort target. 35 Support customers, $7M ARR, Training program live. CNCF Sandbox accepted. L18 decision made. Foundation track and revenue both progressing. Series B optional but supportable."

### Year 3 (M25–M36): category leadership in OSS analytical engine, profitable

**Team scaling.** 47 → 89. Phase 4 → 5.

**Engineering:**
- M28: L11 (compile-time monomorphisation) ships.
- M30: L12 (zero-copy column pipeline) ships. 22q SF=10 geomean ~0.45–0.50 — **V2 Ambitious cohort outcomes achieved.**
- M30: **SF=1000 published bench.**
- M33: ematix-flow is the canonical Iceberg execution engine. Multiple downstream Iceberg consumers cite the project. Sidecar Phase 2 fully adaptive in production for 100+ customer deployments.

**Customer-facing:**
- M27: 60 Support customers. 25 active Consulting engagements. 12 paid BYOC. 500 students/quarter trained.
- M30: 100 customers total. Compliance line live (10+ customers).
- M33: 150 customers total. **Year-3 ARR target $20–30M.**
- M36: 180 customers total. **Profitable** (revenue > burn) or near-profitable.

**Community / Foundation:**
- M27: CNCF Incubating accepted (target).
- M30: 15+ independent committers, 5+ from non-ematix-Inc. organisations.
- M33: ASF Incubator proposal (if pursuing ASF in addition to CNCF; alternative track).
- M36: CNCF Graduated path visible.

**Funding:**
- M36: Cash-flow-positive on bootstrap path (Scenario A), OR Series C optional ($50–100M @ $400M–$800M).

**Year-3 board read:** "V2 Ambitious cohort engineering shipped. $25M ARR, 150 customers, profitable. CNCF Incubating accepted. Iceberg execution engine moat established. Ready for either growth round, acquisition discussion, or independent growth."

### Year 4–5 outcomes (M37–M60)

Three possible trajectories:

**Outcome A — independent growth to maturity.** ARR grows $25M → $60M → $100M+. Headcount 90 → 150 → 250. Profitable throughout. **CNCF Graduated** at Y4. Acquisition offers received and declined unless price band is $1B+. Possible IPO track at $80M+ ARR ($800M–$1.5B valuation, services multiples). **This is the Red Hat-1999-IPO path applied to data infrastructure.**

**Outcome B — strategic acquisition.** Red Hat / IBM / Snowflake / Databricks / Cloudera / Aiven / Confluent / Oracle offers $300M–$1.5B. Board decides based on dilution, founder preferences, OSS continuity guarantees. **Acceptable price band: $500M+**. Crunchy-Data-Snowflake precedent is direct.

**Outcome C — services ceiling hit.** ARR plateaus at $30–50M. Engineering pace slows due to services-delivery cost. Acquisition at $200–500M; OSS project continues under acquirer with reduced ematix-Inc. influence.

**Outcome D — OSS continues without central org.** Worst case: ematix Inc. fails to reach Y3 profitability. Series A doesn't extend. Layoffs at M30. **The OSS engine survives** (Apache 2.0, foundation-governed) under maintainer collective. Some ex-employees continue contribution as individuals or via consulting firms. **The technical impact is real, but the OSS continues.** This is the structural guarantee of pure-OSS posture: the project doesn't die with the company.

Outcome D is **the structural reason V4 is a different bet than V3.** A failed V3 means the cloud product evaporates and the OSS engine loses its main maintainer. A failed V4 means the company is gone but the OSS engine is foundation-governed and survives.

---

## Part 7 — Risks and pre-mortem (V4-specific)

V3's pre-mortem (Part 7) catalogued 10 failure modes. Many carry forward. V4 adds 3 services-business-specific failure modes and adjusts severity for several others.

### Carried forward from V3 (with V4 severity adjustment)

**FM-1 — Conway's Law: 30+ engineers couldn't ship coherent product.** V4 severity: **lower** than V3 because the engineering team is smaller (32 vs 50) and tracks are fewer (no Track G). Mitigation: same as V3 (architectural board, ADRs, cross-track rotation).

**FM-2 — Hyperscaler / incumbent copied the wedge.** V4 severity: **same** as V3. Mitigation: ship multiple wedges (engine perf + Iceberg-strategic-centre + learning optimizer + pure-OSS posture).

**FM-3 — Σ.L learning loop is curiosity, not a wedge.** V4 severity: **lower** than V3. In V3, Track D needed to deliver wedge-quality customer attestation to justify cloud-product positioning. In V4, the learning optimizer is one of three wedge claims; if it underperforms, the Iceberg moat + cluster-scale story still carries.

**FM-4 — DuckDB / MotherDuck adds distributed.** V4 severity: **same** as V3. Mitigation: be 2 years ahead via SF=100/SF=1000 publication + Iceberg-native discipline.

**FM-5 — L18 fork burns engineering, no customer-visible features.** V4 severity: **higher** than V3 because V4 has fewer engineers to absorb the fork cost. Mitigation: harder M18 gate criteria; fork only if absolutely necessary.

**FM-6 — Iceberg execution layer commoditises.** V4 severity: **same.** Differentiate on *how* we use Iceberg (write-side tuning, sidecar, learning loop).

**FM-7 — Pre-hiring with wrong people.** V4 severity: **lower** because V4 hires more slowly (89 by M36 vs V3's 88 by M18). The 18-month delay buys hiring discipline.

**FM-8 — Cloud product team underestimated multi-tenant ops.** V4 severity: **n/a** — no cloud product.

**FM-9 — Series B/C raise climate worsened.** V4 severity: **lower** because V4 doesn't require Series B/C for survival. Series A funds bootstrap to revenue. Macro doesn't kill the company.

**FM-10 — L18 fork decision wrong direction.** V4 severity: same as V3.

### V4-specific new failure modes

#### FM-11 — Services revenue plateau

**Story.** By M36, ARR is $12M instead of $25M. Net retention is 95% (not 115%). New-logo growth is slower than projected because the OSS analytical-engine services market is smaller than the OS-services market Red Hat addressed. ematix Inc. cannot fund Phase 4 + 5 hiring; growth stalls at 50 employees. Engineering pace slows further. Engine perf gap to DuckDB / Snowflake widens. **The "ceiling around $30–50M ARR" risk materialises early.**

**Why it might happen.**
- Total spend on analytical-engine services (vs OS services or DB services) is structurally lower because analytics-team budgets favour SaaS warehouses.
- Customers who want "expert help" go to system integrators (Capgemini, Accenture) who package multiple vendors, not single-vendor specialists.
- Self-hosters who can run ematix-flow are also the customers least likely to want a paid support relationship (engineering-strong, want to handle their own ops).
- The natural ARR ceiling for an analytical-engine-services-company may be $30–50M not $100M.

**Probability:** Medium-high. The PostgreSQL services market is $300–500M (across EnterpriseDB, Crunchy, Percona, smaller). The analytical-engine services market is smaller (Starburst's enterprise + Altinity + a few smaller players sum to <$200M).

**Mitigation:**
1. **BYOC line as growth bridge.** §2 Line D (managed-in-customer-account) provides ARR-per-customer 2–3× Support alone. Pursue it aggressively if Support line plateaus.
2. **OEM / embedded licensing.** §2 Line F. ematix-parquet adoption by ISVs creates a non-services revenue line that scales differently.
3. **Sponsorship / foundation revenue.** §2 Line G. Hyperscaler / large-enterprise sponsorships are 100% margin and can grow faster than direct customer revenue.
4. **Acceptance.** The honest mitigation may be: **plan for $50–100M ARR as the natural ceiling, not Snowflake-scale**. Operate accordingly.

#### FM-12 — Hyperscaler launches "Amazon ematix" and captures 80% of addressable market

**Story.** At M18, AWS announces "Amazon ematix" as a managed service. By M30, 60% of new ematix-flow workloads start on AWS managed. ematix Inc.'s direct customer acquisition slows because customers default to AWS managed. Support contract upsell still works but at lower rates than projected. ARR plateaus at $15–20M.

**Why it might happen.**
- AWS has shipped managed services of every important OSS data infrastructure (Postgres, MySQL, Kafka, ClickHouse, Trino, Spark, OpenSearch). ematix-flow is plausibly next once it crosses adoption threshold.
- Customers default to AWS-bundled-service for procurement simplicity.
- The "we are the deepest experts" mitigation is real but limited — most customers don't need engine-internals support; they need basic ops.

**Probability:** Medium. AWS launches managed services of high-adoption OSS, not low-adoption. If ematix-flow has the adoption to justify AWS managed (say 10K+ GitHub stars + production customers), it has enough adoption that ematix Inc.'s services are viable in parallel. Confluent + Kafka-on-MSK is the precedent: Confluent makes $400M+/year ARR despite MSK existing.

**Mitigation:**
1. **Partnership over competition.** Engage AWS / GCP / Azure database-services teams from M0 (DevRel role). Offer reference architectures, integration assistance, support contracts to managed-service teams. **Become the AWS-MSK-to-Confluent equivalent for AWS-ematix-to-ematix-Inc.**
2. **Deep-expert positioning.** ematix Inc.'s value proposition is engine internals, not basic ops. Build certification / training program that emphasises depth. Hyperscalers cover breadth.
3. **Foundation track is the structural mitigation.** A foundation-governed project is harder for any single hyperscaler to "own." It belongs to the community.
4. **Accept the upside.** Managed services drive adoption. Adoption expands the support market. Net effect over 5 years is plausibly positive even if any individual quarter looks competitive.

#### FM-13 — Open-source community doesn't materialise

**Story.** By M24, GitHub stars are at 3K (vs 5K target). External contributors are <5; nearly all PRs are from ematix Inc. employees. The "OSS community" framing is fiction; the engine is effectively a single-vendor proprietary engine with a permissive licence. Foundation track stalls because CNCF Sandbox reviewers see no independent governance.

**Why it might happen.**
- Modern OSS has high bar for community building — requires deliberate investment in contributor experience (RFC process, mentorship, good first issues, contribution guides).
- Analytical-engine OSS is hard to contribute to casually because changes touch performance, correctness, distributed semantics. Drive-by PRs rarely viable.
- DevRel + community engineering is undersized at 3 ICs.

**Probability:** Medium-high. Most OSS projects fail to build real communities; they end up as single-vendor projects with permissive licences. ClickHouse (under Yandex) struggled with this until Altinity spun out. DataFusion has stronger community structure (Apache governance) but it took years.

**Mitigation:**
1. **Track H is fully funded from M0.** DevRel lead in first 6 hires. Community engineering treated as senior IC work, not pure marketing.
2. **Explicit RFC process from M0.** Every cross-track decision goes through public ADR / RFC. Lowers contributor bar.
3. **Contributor mentorship program.** 1:1 mentor pairings for first-time contributors. Annual "ematix-flow Contributors Summit" funded from Year 2.
4. **Foundation track is the forcing function.** CNCF Sandbox requires diversity of contributors; pursuing it deliberately seeds the community-building work.
5. **Customer-driven contributions.** Support contract customers who develop custom operators / connectors are invited (and supported, via Field Eng) to upstream them. **This is the highest-leverage community-building motion**: paid customers who actually use the engine contribute high-quality PRs back. Builds the contributor base and the customer relationship simultaneously.

#### FM-14 — Founders / leadership unwilling to operate a $50M-revenue company

**Story.** By M30, ARR is $20M, growth is healthy but the founders / early team are temperamentally unsuited to a "profitable services business" trajectory. They want category-redefining outcomes (Snowflake-scale). They burn out, leave, or pivot the company to add a cloud product (V3 retrofit), splitting focus mid-execution.

**Why it might happen.**
- Founder ambition + investor pressure both push toward maximum-scale outcomes.
- A profitable $20M-ARR services company is "below the venture-scale threshold." Series A investors may push for cloud-product pivot.
- The Red Hat path requires *patience* (Red Hat took 15 years from founding to $1B revenue). Founder temperament must match.

**Probability:** Medium. The honest assessment: V4 only works if the founders / board genuinely embrace the Red Hat path. If they have lingering Snowflake-scale ambitions, V4 is the wrong structure.

**Mitigation:**
1. **Founder-board alignment in M0.** Make the V4 trajectory explicit in board materials. Set expectations: $300M–$1.5B exit band, not $3B+.
2. **Choose investors aligned with the model.** Some VCs prefer infrastructure-services bets (e.g., Battery Ventures, Insight Partners' more conservative arm). Some demand SaaS multiples. Pick the former.
3. **Pre-commit governance.** Foundation track (CNCF / ASF) makes a mid-flight pivot to "cloud product + BUSL" structurally harder. Acts as a commitment device.

---

## Part 8 — Recommendation

### The one sentence

**Pursue pure-OSS Apache 2.0 ematix-flow + a Red Hat-style services company (ematix Inc.) at 30–40 engineers + 36 customer-facing employees + 21 G&A/exec = ~90 total by Year 3; fund with $10–20M Series A at $40–80M valuation, no required Series B; target $20–30M ARR by Year 3 and profitability by Year 4; ship V2 Ambitious cohort technical outcomes in 24–30 months (vs V3's 18); pursue CNCF Sandbox → Incubating → Graduated path in parallel; accept Y4–Y5 exit band of $500M–$1.5B as the realistic upside.**

### Viability assessment: yes, conditionally

V4's central question: **can ematix-flow ship the V2 Ambitious cohort outcomes on a Red Hat-style revenue model at 30–80 employees?**

**Answer: yes, with three explicit conditions:**

1. **Calendar extends from 18 to 24–30 months for full V2 Ambitious outcomes.** Track A's L11 + L12 ship in M28–M30 instead of M15–M18. SF=100 cluster bench still publishes in M11–M12 (only 2–3 months later than V3). SF=1000 publishes in M27–M30.

2. **The customer-facing organ must be fully funded from Year 1.** Field Engineering, Customer Success, Training — these are not afterthoughts. Underfunding them means engineering builds the wrong things and revenue ramps slowly.

3. **Founders + board must genuinely embrace the Red Hat trajectory.** $300M–$1.5B exit band, not Snowflake-scale. Profitable services business, not venture-pressured cloud product. If this commitment is not real, V4 is the wrong structure and V3 is the right answer.

### Realistic Year-3 ARR

**$20–30M ARR with high confidence (60–70% probability).** Distribution:
- Median: $22M.
- 25th percentile: $14M (FM-11 partial materialisation).
- 75th percentile: $32M.
- 10th percentile: $9M (FM-11 + FM-13 both materialise).
- 90th percentile: $45M (foundation + sponsorship lines outperform).

Below $10M ARR at Y3 is a *failure* trajectory triggering layoffs or strategic acquisition at $200–400M.

### Realistic exit / outcome

Three scenarios, with rough probability:

1. **Profitable bootstrap to maturity (45% probability):** Y3 $20–30M ARR, Y5 $60–100M ARR, profitable throughout, optional IPO at Y5–Y6 valuation $800M–$1.5B (services multiples). **Red Hat-1999-pattern.**

2. **Strategic acquisition at Y3–Y5 (35% probability):** Acquirer pays $500M–$1.5B for the engine + Iceberg moat + customer base. Acquirers most likely: Red Hat / IBM (engine + Apache foundation discipline matches their model); Snowflake (Crunchy precedent); Databricks (consolidate Iceberg execution layer); Cloudera; Aiven. **Crunchy-Data-Snowflake-2024 precedent.**

3. **Services-ceiling outcome or company failure (20% probability):** ARR plateaus at $15–25M or company fails to reach profitability. OSS engine survives under foundation governance. Original team partly retained by acquirer (low-value acquisition $200–400M) or disperses. **The pure-OSS posture means the technical legacy survives even in this outcome.**

### Apache Foundation / CNCF route — yes, pursue

The foundation track is **strategically valuable and worth the investment cost (~20% of one engineer + 50% of DevRel lead).** It does three things:

1. **Lowers procurement friction** for risk-averse enterprise buyers who got burned by BUSL conversions.
2. **Acts as commitment device** preventing mid-flight pivots to proprietary tiers under investor pressure.
3. **Unlocks sponsorship revenue** (§2 Line G) from hyperscalers and large enterprises who fund foundation projects but not single-vendor projects.

**Recommended path: CNCF Sandbox → Incubating → Graduated, with ASF as an alternative considered at Year 2.** CNCF fits ematix-flow's cloud-native architecture; ASF fits the DataFusion + Iceberg integration. Pick the track in Year 2 based on community development; pursue Sandbox in Year 1 regardless.

### What to tell the board at each milestone

- **M3:** Team scaled to 15. First 8 hires onboarded. V2 Phase T1 outcomes shipped. SF=100 harness running.
- **M9:** Team at 27. First 5 paying Support customers. SF=100 bench close to publication. CNCF Sandbox proposal in.
- **M12:** SF=100 published. Series A closed at $10–20M. ARR ~$1M.
- **M18:** L18 fork decision made (defensibly). 22q SF=10 below DuckDB on V2-Moderate metrics. 20+ Support customers. ARR ~$5M. CNCF Sandbox accepted.
- **M24:** Team at 47. Training program live. L17 production learning loop shipped. ARR ~$10M. Foundation Incubating proposed.
- **M30:** V2 Ambitious cohort shipped. SF=1000 published. 100+ customers, $20M ARR.
- **M36:** Team at 89. $25M ARR, profitable. CNCF Incubating accepted. Decision point: independent growth, growth round, or strategic acquisition.

### What we explicitly are NOT doing

- **Not building a SaaS product.** No multi-tenant compute. No per-query billing.
- **Not adopting BUSL/SSPL.** Apache 2.0 forever, written into company charter.
- **Not pursuing $3B+ exit outcomes.** Trade for sustainable burn + foundation governance + slower dilution.
- **Not pre-hiring beyond proof gates.** Phase 2 → 3 → 4 → 5 hiring is gated.
- **Not entering embedded SQL.** MotherDuck owns it.
- **Not competing for federated query workloads.** Starburst owns it.
- **Not operating customer compute at multi-tenant scale.** BYOC is single-tenant, customer-owned.

### What we explicitly ARE betting on

The bet is that **a pure-OSS engine + a focused services company can sustain a profitable $30–60M revenue business indefinitely, with foundation governance ensuring project survival regardless of company outcome.** The Red Hat pattern proved this works for OS infrastructure ($3B revenue at IBM acquisition). EnterpriseDB / Acquia / Altinity prove it works at smaller scale for adjacent infrastructure ($30–200M revenue ranges). The bet is wrong if (a) the analytical-engine services market is structurally smaller than these comparables (FM-11), (b) hyperscaler cloning captures more than it expands the support market (FM-12), or (c) the OSS community fails to materialise into independent contributors (FM-13).

The bet is correct if (a) ematix-flow becomes the canonical OSS Iceberg execution engine and customers running production workloads on it want expert support relationships, (b) the learning optimizer's customer attestation creates a category of "self-organising warehouse on your own infrastructure" with a wedge story DuckDB / Snowflake / Databricks cannot match without changing their business models, and (c) the Apache 2.0 / foundation-governed posture lowers procurement friction enough to convert OSS adoption into Support / Consulting / Training revenue at the rates §2 projects.

### The conditional alternative

If the architect's conclusion is that V4 is **not viable** at 30–80 employees with the V2 Ambitious agenda, the alternatives are:

1. **Smaller-scope V4 (recommended fallback).** 15–25 employees, slower technical agenda (V2 Moderate cohort instead of Ambitious; SF=100 ships but SF=1000 deferred), smaller revenue ceiling ($10–30M ARR by Y3–Y4). Corresponds to the Crunchy Data / earliest-Altinity scale. Still profitable, still pure-OSS, still foundation-track-eligible.

2. **Hybrid V4-with-BYOC-emphasis.** 40–60 employees. Same as V4 but the BYOC line (§2 Line D) is scaled aggressively — 50–100 BYOC customers by Y3 generating $15–30M ARR from BYOC alone. Total ARR $40–60M by Y3. This pulls toward V3's economics without crossing into multi-tenant SaaS. Boundary case worth considering.

3. **Accept that V3's cloud product is required for the V2 Ambitious agenda.** If the architect / founders conclude that 18-month delivery of L11 + L12 + L15 + L17 is non-negotiable, the burn rate requires V3's cloud-product-funded ARR. Return to V3.

**The architect's recommendation is V4 as written: ship the V2 Ambitious agenda in 24–30 months on services revenue, accept the slower calendar, accept the $500M–$1.5B exit band, gain the foundation-governance benefit and the pure-OSS values alignment.** This is the most defensible answer to the user's V4 question.

---

## References

- V1 (`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE.md`) — per-query root cause, lever menu L1–L7, codegen-tax-constrained sequencing.
- V2 (`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V2.md`) — full L1–L18 menu, Conservative/Moderate/Ambitious cohorts, strategic discussion (§5).
- V3 (`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V3.md`) — 50-engineer B+D hybrid, cloud product, $50–80M Series B funding shape.
- Sidecar plan (`docs/plans/CURRENT.md`) — Phase 1 read-side + Phase 2 adaptive, the substrate for Track B.
- `ematix.dev/concepts/why-ematix-flow.mdx` — current wedge positioning (8 numbered claims; V4 §1.1 reframes for pure-OSS).
- Memory `project_distributed_is_shipped.md` — distributed batch SQL is shipped; the SF=100 publication is the dominant Year 1 milestone.
- Memory `project_sigma_l_adaptive_runtime.md` — Σ.L 32-test pipeline; foundation for Track D + L17.
- Memory `project_sigma_q_l13_to_l16_session.md` — current 22q SF=10 geomean 0.80 baseline.
- Memory `project_optimizer_codegen_sensitivity.md` — codegen tax; PGO infra + sibling-crate discipline.
- Memory `project_ematix_parquet_repo.md` — sibling-crate model; basis for Line F OEM revenue.
- Comparables: EnterpriseDB ($200M revenue, 700 employees, 25 years); Acquia ($200M, 600, 17 yrs); Altinity ($30–50M, 120, 9 yrs); Crunchy Data ($30M+, 100, 13 yrs, acquired by Snowflake 2024 ~$300M); Percona ($50M+, 300, 18 yrs); Red Hat pre-IPO (1997: $10M revenue; 1999 IPO at ~$80M revenue / $400M market cap; 2019 IBM acquisition $34B).
- Foundation governance precedents: Apache Arrow, Apache DataFusion, Apache Iceberg, Apache Kafka, Apache Spark (all ASF); Kubernetes, Prometheus, etcd, Envoy (all CNCF Graduated).
- Recent acquisition comparables (services tier): Crunchy Data → Snowflake ~$300M (2024); Mesosphere → D2iQ ~$100M (services pivot, 2019); Sysdig acquisitions in observability ($100–500M range).

---

*End of V4. V4 takes positions V3 left open about whether the services-only model is viable, and accepts the trade-offs explicitly: longer calendar to V2 Ambitious outcomes, smaller exit band, sustainable burn, foundation-governance optionality. If the V4 trade-offs are unacceptable, V3 is the right answer; if they are values-aligned, V4 is the most defensible plan at 30–80 employees on Red Hat-pattern revenue.*
