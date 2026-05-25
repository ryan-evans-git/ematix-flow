# Σ.T (V3) — strategy at scale, 50 engineers and $30–50M/year

**Status:** strategic plan (board-readable, not a release artifact)
**Date:** 2026-05-25
**Author:** architect agent (cold-read, no main-thread context)
**Branch:** `perf/sigma-q-single-node-parity` at HEAD `dc2d457`
**Predecessors:**
- V1 (`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE.md`) — 2-engineer, codegen-tax-constrained, conservative menu.
- V2 (`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V2.md`) — 2–3 engineer, scope unrestricted, recommended Moderate cohort.

**What V3 is.** V3 is not a longer V2. V2's question was "given scarce engineering, which levers should we pick." V3's question is **"given that engineering is no longer the binding constraint, what is the strategy that wins markets at scale."** The hard part stops being "which kernel to write" and becomes "where to compete, what to build, who to hire, how to monetise, who eats us if we get it wrong." The technical agenda survives — V2's L1–L18 are still the engine menu — but its sequencing is now determined by which strategic frame the company adopts, not by which lever has the lowest blast radius.

**The board-readable answer in one sentence.** ematix-flow at 50 engineers and $30–50M/year should pursue **Option B+D hybrid** — a commercial cloud product on top of an OSS distributed SQL engine, with the open Iceberg+sidecar table-and-execution layer as the moat against Snowflake/Databricks, the learning optimiser as the durable wedge against DuckDB/MotherDuck, and a four-tier hiring sequence (5→15→25→50 over 18 months) gated on production proof points rather than pre-hiring.

---

## Table of contents

- [Part 1 — Strategic frame (where do we actually compete)](#part-1--strategic-frame-where-do-we-actually-compete)
- [Part 2 — Technical agenda (V2 reframed for parallel tracks)](#part-2--technical-agenda-v2-reframed-for-parallel-tracks)
- [Part 3 — Organizational structure](#part-3--organizational-structure)
- [Part 4 — Capital and business model](#part-4--capital-and-business-model)
- [Part 5 — Competitive deep-dive](#part-5--competitive-deep-dive)
- [Part 6 — Year-by-year milestones](#part-6--year-by-year-milestones)
- [Part 7 — Risks and pre-mortem](#part-7--risks-and-pre-mortem)
- [Part 8 — Recommendation (the one-page board read)](#part-8--recommendation-the-one-page-board-read)

---

## Part 1 — Strategic frame (where do we actually compete)

### 1.1 The four positioning options

V2 §5 made the strategic question explicit but stopped at "the cohort depends on which frame applies." V3 is the doc that **picks the frame.** Four options, scored:

| Axis | A: OSS distributed SQL for self-hosted data lakes | B: Commercial cloud product on OSS engine | C: Embedded SQL for ISV data platforms | D: Iceberg+execution layer ("the open warehouse") |
|---|---|---|---|---|
| **TAM** | $4–6B (Trino/Starburst category) | $20–40B (Snowflake/Databricks category) | $1–2B (DuckDB/MotherDuck adjacency) | $6–10B (table-format-plus-engine; Tabular precedent valuation) |
| **TAM growth** | 15% CAGR (Trino is mature, Starburst is plateauing) | 30–40% CAGR (warehouse is still expanding) | 40%+ CAGR (embedded analytics is a new category) | 50%+ CAGR (Iceberg is currently the fastest-growing format) |
| **Decision-maker density** | 1 per data-platform team @ ~5000 companies = 5K decision-makers | 1 per CTO/VP-Data @ ~50K companies = 50K | 1 per ISV product team @ ~5K ISVs = 5K | 1 per data architect @ ~3K enterprises = 3K |
| **Sales cycle** | 6–18 mo (enterprise, infra-team-driven) | 1–6 mo (self-serve trial → expansion) | 3–9 mo (ISV procurement) | 12–24 mo (architecture committee) |
| **Defensibility vs Snowflake/DBX** | Low — they ship their own Trino-equivalent | Low without a wedge — they have us on cost; we need 10× cost-perf | Medium — they don't ship embedded | **High** — Databricks owns Iceberg (Tabular acq) but the *execution layer* is open |
| **Defensibility vs DuckDB/MotherDuck** | Medium — DuckDB doesn't distribute | Medium — MotherDuck might add distributed mode | **Low** — they're the incumbent | Medium — they don't own table format |
| **Cost-to-win-first-customer** | $50–200K sales+SE cost; 6mo cycle | $5–20K via self-serve PLG; 1mo cycle | $100K SE; 3mo integration | $200–500K; 12mo cycle, architecture-board sale |
| **Founder/team fit** | High — we have the engine and the harness | Medium — we have no PLG / billing / multi-tenant ops | Medium — we have no embedded SDK story | High — we have ematix-parquet + sidecar + Σ.L |
| **What stops a copy** | Nothing — Trino is Apache-licensed; the engine is replicable | Time-to-product + brand; not technical | DuckDB's brand + simplicity | **Owning the table format spec + the reference engine that uses it best** |

**Scoring summary:**
- Option A loses on defensibility. Starburst tried this; they sold for $3.35B but at a TAM ceiling. We'd be Starburst-the-sequel.
- Option B is the **highest-TAM single option** but requires building everything Snowflake has (multi-tenant, billing, auth, ops, support) while also out-shipping them on engine perf. Capital-intensive. High burn justified.
- Option C is the cleanest founder-fit but the lowest TAM. MotherDuck owns it already. Hard to win without becoming MotherDuck-but-with-distributed, which is a 3-year catch-up.
- Option D is the **highest defensibility single option** but the longest sales cycle. Owning the open table format + the execution layer is the position Databricks tried to acquire (Tabular) and that the broader Iceberg community is still un-committed to a single vendor for. There is a real architectural opening here in 2026.

### 1.2 The recommendation: Option B + D hybrid (not A, not C)

**A pure-Option-B play is what Snowflake and Databricks already are; we lose head-on.** Their PLG funnels, their sales orgs, their integrator partnerships outclass anything a Series B can spin up in 18 months. The only Option B variant that wins is one with a **structural cost-perf advantage that the incumbents cannot replicate without breaking their own business model**. That advantage is owning the open storage layer (Option D) — the layer below them — so that customers can migrate to ematix-flow without migrating their data out of S3/GCS Iceberg.

**A pure-Option-D play has the right defensibility but no commercial flywheel.** Iceberg is an Apache project; we don't own the spec. We can own the *reference execution engine* for it, but reference-engine-without-cloud is an open-source category and OSS distributed engines don't generate the revenue needed to justify $30–50M/year burn (see Part 4). Apache Pinot, Trino, and ClickHouse-OSS all stayed sub-$50M ARR for years before their commercial siblings (StarRocks, Starburst, ClickHouse Cloud) shipped.

**The hybrid that wins.** Option D for moat + Option B for revenue. Concretely:

> **ematix-flow is the open-source query engine + sidecar storage layer that runs Iceberg tables 5–20× cheaper than Snowflake/Databricks. ematix Cloud is the hosted version of the same engine, sold to mid-market and growth-stage companies who want Snowflake-tier perf at OSS-tier cost.**

The OSS engine is the recruiting tool, the integration surface, and the moat. The cloud product is the revenue engine. Customers run the OSS engine on their own laptops + their own dev environments; when they want production scale + multi-tenant + zero-ops, they upgrade to ematix Cloud on the same Iceberg tables they were already querying.

This is the **Confluent / Databricks / ClickHouse-Cloud / Snowflake pattern** but with two specific differentiators:

1. **The OSS engine is genuinely competitive with the cloud product on perf** — not a crippled OSS tier. We don't withhold features; we charge for ops + scale. This makes the OSS adoption story honest (cf. Elastic and MongoDB BUSL controversies).
2. **The storage layer is open Iceberg, not a proprietary format.** Customers' data sits in their S3/GCS. We never own customer data. This eliminates the lock-in objection that has slowed Snowflake's growth into Fortune 500 IT-skeptical buyers.

### 1.3 Three-year product roadmap consistent with the recommendation

The hybrid frame implies a three-track product:

**Track I — ematix-flow OSS (the recruiting and moat layer).** Year 1: V2 Ambitious cohort outcomes (SF=10 22q geomean 0.45–0.55) + sidecar Phase 2 + Iceberg read-side + cluster SF=100 published bench. Year 2: write-side Iceberg + adaptive layout (Σ.L.5) + GPU offload tier. Year 3: it is the canonical Iceberg execution engine in the market.

**Track II — ematix Cloud (the revenue layer).** Year 1: design-partner pilots running on a thin control plane (cluster auto-scaling + billing + auth + ops dashboard). Year 2: GA with self-serve onboarding, multi-tenant isolation, $5–20M ARR. Year 3: $30–100M ARR, $250–500M valuation territory.

**Track III — ematix Iceberg+ (the wedge).** Year 1: sidecar generation tooling that any Iceberg consumer can adopt (open-source `ematix-iceberg-sidecar` crate). Year 2: official Iceberg `attach_extension` ratification (or de-facto adoption via critical mass). Year 3: the open storage layer becomes the "Linux of analytic storage," and ematix Cloud is the canonical execution layer.

If Track III fails (Iceberg consolidates around Databricks' Tabular-derived reference impl), Track I still has the engine play + Track II still has the cloud play. If Track II fails (cloud market saturated, ematix can't differentiate enough to pull customers from Snowflake), Track I+III still produce a defensible OSS category leader. **The hybrid is also a hedge.**

### 1.4 Why this isn't dilutive

The standard critique of a "pick two" strategy is that the team loses focus. The reason it isn't dilutive here:

- **Tracks I + III share 90% of the engineering surface.** The OSS engine and the Iceberg+sidecar storage layer are the same codebase. Track III is a positioning + spec + community-engagement effort, not a separate engine.
- **Track II is the only commercial-product surface.** Multi-tenant control plane, billing, ops dashboard — these are net-new, but they're net-new for *any* commercial direction. Option B alone would still require them.
- **The PLG funnel is OSS → Cloud.** Users adopt the OSS engine, find that their workload outgrows the laptop / single VM, upgrade to Cloud. That funnel only works if both layers exist. So the "two-track" framing is misleading — it's one product (OSS → Cloud) with a moat (open storage).

The diluted version of this would be Option A + B + C + D, where we ship OSS distributed + Cloud + embedded SDK + open table format simultaneously. That's four sales motions, four buyer personas, four engineering surfaces. We're explicitly **not** doing that. **Option C is rejected.** Embedded SQL is MotherDuck's market; we don't enter it.

---

## Part 2 — Technical agenda (V2 reframed for parallel tracks)

V2's L1–L18 are not re-derived. They are the input. V3 reframes them as fired in parallel across dedicated engineering tracks, with the codegen-tax constraint relaxed by having a dedicated infra team that maintains PGO + sibling-crate discipline as a service to the rest of engineering.

### 2.1 The tracks (8 engineering teams + 1 supporting)

| Track | Team | HC | What ships | Strategic role |
|---|---|---:|---|---|
| **A** | Engine core | 8–10 | L3 PGO infra, L8 CBO (sibling crate), L9 Cranelift JIT prototype, L10 dynamic filter propagation, L11 compile-time monomorphisation, L13 custom hash join, L12 zero-copy column pipeline, L18 DataFusion fork (decision deferred to month 12) | Track I + Track II perf — the "10× cheaper" claim |
| **B** | Storage | 5–6 | L15 read-side Iceberg in production, sidecar Phase 2 adaptive auto-creation, write-side manifest gen, partial materialised views, Z-order layouts, write-side sidecar generation | Track III moat — owning the open layer |
| **C** | Distributed | 6–8 | Cross-host SF≥100 bench published, Σ.J Flight transport hardening, cluster auto-scaling, multi-tenant isolation, peer-discovery service, distributed shuffle-join with L13 | Track I + Track II — the "scales beyond one node" claim |
| **D** | Adaptive runtime | 4–5 | Σ.L observer productionisation, L17 learning optimiser wire-up, Σ.L.5 write-side tuner, sidecar-Phase-2 scoring integration | The "engine that learns" wedge story |
| **E** | GPU | 3–4 | L16 Metal prototype → NVIDIA portability, GPU offload integration with engine, cluster GPU instances bench | Track II premium tier ("ematix Cloud GPU") |
| **F** | DX (developer experience) | 5–6 | Python SDK polish, Rust crate API, CLI ergonomics, Web UI feature parity with Snowflake/Databricks single-node surface, observability stack, error messages, docs site (ematix.dev) | Track I adoption — the funnel that feeds Track II |
| **G** | Cloud product | 8–10 | Multi-tenant control plane, billing, auth, autoscaling, ops dashboard, customer onboarding flow, Stripe integration, SSO, audit-log surface, status page | Track II — the entire revenue layer |
| **H** | DevRel + community | 3–4 | Conference talks (re:Invent, KubeCon, Data+AI Summit, P99 CONF), OSS engagement (Iceberg community, DataFusion contribution, Apache PRC), ecosystem integrations (dbt-core adapter, Mode/Hex connector), benchmark publishing | Track III community + Track I + Track II top-of-funnel |
| **I** (supporting) | Infra/SRE | 4–5 | CI/CD scale-out, bench-on-every-PR @ SF=10 + SF=100, PGO build infra, release automation, internal observability, Iceberg test fixture mgmt | All tracks depend |

**Total engineering: 48–58.** Target 50, with the band reflecting the natural variance in hiring.

### 2.2 Strategic dependencies (Gantt-equivalent)

```
M0 ─ M3 ─ M6 ─ M9 ─ M12 ─ M15 ─ M18 ─ M21 ─ M24

Track A (engine):
  L1 sidecar P1 + L3 PGO + L6 ──> L13 kernel ──> L8 CBO spike ──> L8 prod ──> L10 ──> L11 ──> L12 ──> (L18 decision @ M12) ──> L18 if go
                                                                                                          |
Track B (storage):                                                                                        |
  L15 read-side Iceberg ──> sidecar P2 ──> L15 write-side manifest ──> Σ.L.5 wire-up ──> partial MVs ──> Z-order layouts
                |                                                  |
Track C (distributed):                                              |
  SF=100 harness ──> SF=100 published bench ──> SF=1000 prep ──> SF=1000 bench ──> multi-tenant isolation ──> cluster auto-scaling
                              ^                                                              |
                              | (this is the dominant strategic milestone — M9 target)       |
                                                                                              |
Track D (adaptive):                                                                           |
  Σ.L.2 wire-up ──> L17 production loop ──> sidecar P2 scoring ──> Σ.L.5 write-side ──> production learning
                              ^                                                              |
                              | (depends on Track A's L8 CBO existing for L17 to consume)    |
                                                                                              |
Track E (GPU):                                                                                |
  Metal prototype ──> NVIDIA port ──> Engine integration ──> Cluster GPU bench
                                                                                              |
Track F (DX):                                                                                 |
  Python SDK ──> Rust crate stabilisation ──> Web UI parity ──> ematix.dev re-launch ──> docs versioning
                                                                                              |
Track G (cloud):                                                                              |
  Design partner pilots ──> Stripe + auth + ops dashboard ──> GA (M18) ──> billing-tier expansion ──> SSO + audit ──> enterprise tier
                                       |                                                      |
                                       | (M9: first design partners paying)                  |
                                                                                              |
Track H (DevRel):                                                                             |
  Conference circuit + Iceberg engagement ──> bench-publishing cadence ──> ecosystem integrations ──> ematix-summit (Y2)
                                                                                              |
Track I (infra):                                                                              |
  CI/CD scale-out ──> bench-on-every-PR ──> PGO build infra ──> internal observability + alerting
```

**Critical-path dependencies (the slip-cascade map):**

- Track A's L13 (custom hash join) is the input for Track C's distributed shuffle-join. **If A slips L13 by 6 weeks, C's SF=100 bench slips 6 weeks.**
- Track B's L15 read-side Iceberg is the input for Track G's cloud product. **If B slips L15 by 8 weeks, G can't onboard design partners on Iceberg-native plans for 8 weeks** (Track G can ship a non-Iceberg pilot using DataFusion default, but the wedge story is missing).
- Track D's L17 depends on Track A's L8 existing. **If A slips L8 by a quarter, D's learning loop is also delayed a quarter.**
- Track C's SF=100 bench publication is the dominant strategic milestone — see Part 6 Year 1. **If C slips this past M9, the funding narrative for Series B/C is materially weaker.**

These four dependencies are the only cross-track critical-path items. Everything else can slip independently without cascade.

### 2.3 Headcount, calendar, and risk per track

| Track | HC | Calendar to MVP | Calendar to production | Strategic-slip risk | Mitigation |
|---|---:|---:|---:|---|---|
| A engine | 8–10 | M4 (L13 + L8 spike) | M9 (L8+L10+L13 prod) | Medium — L8 CBO is the most ambitious single deliverable | Sibling-crate discipline; PGO infra (Track I); start L8 spike in M2 |
| B storage | 5–6 | M3 (Iceberg read-side) | M9 (sidecar P2 + write-side) | Medium — Iceberg write-side semantics are subtle | Hire a senior Iceberg-experienced engineer in the first 6 hires; talent acqui-hire (see §2.5) |
| C distributed | 6–8 | M6 (SF=100 bench harness) | M9 (SF=100 published) | **High — this is the strategic milestone** | Dedicate the most senior distributed-systems engineer; budget contingency for hardware (Track I) |
| D adaptive | 4–5 | M9 (after Track A L8) | M12 (production loop) | Low — Σ.L substrate already shipped | Gate on Track A L8 ship; don't pre-commit calendar |
| E GPU | 3–4 | M6 (Metal prototype) | M15 (cluster GPU bench) | Medium — portability across NVIDIA/AMD is real work | Defer the prod tier to Year 2; Year 1 ships only the Metal prototype + bench |
| F DX | 5–6 | M3 (Python SDK + Web UI parity) | M9 (versioned docs, full ecosystem) | Low — well-understood work | Standard product-team cadence |
| G cloud | 8–10 | M6 (design-partner pilot) | M18 (GA) | High — multi-tenant control plane is a real product | Hire a senior cloud-product EM in first 10 hires; consider acqui-hiring a small SaaS team |
| H DevRel | 3–4 | M3 (first conference talk) | continuous | Low | Hire a known DevRel from the data space in first 8 hires |
| I infra | 4–5 | M3 (CI/CD at 50-eng scale) | M6 (bench-on-PR) | Low | Standard SRE practice |

### 2.4 What changes from V2 (the framing shift)

V2's recommendation was Moderate cohort (L1+L2+L3+L4+L6 + L8+L13 in 6 months). V3 reframes:

- **L1+L2+L4+L6 ship in M1–M3** — they're not the strategy, they're the warm-up while the larger tracks staff up.
- **L8+L10+L13 are the engine team's first-half M4–M9** — the platform investment that V2 sized at 6 months and V3 commits to fully.
- **L11+L12 are the engine team's second-half M10–M18** — V2 deferred these to "post-decision"; V3 commits them in parallel with the cloud product (Track G) ramp.
- **L15 is a full track (Track B) starting day 1** — V2 sized this as 3–4 person-quarters with a year+ horizon; V3 makes it a permanent storage team that owns Iceberg integration end-to-end.
- **L17 is a full track (Track D)** — V2 had it as a "follow-up after L8"; V3 has it as the team that productionises the learning wedge story.
- **L18 (fork) decision deferred to M12** — V2 said "don't do this." V3 says "decide at M12 based on whether the L8+L10+L13+L11+L12 stack has hit the codegen / extensibility wall." If yes, the engine team forks. If no, we stay on DataFusion as the upstream and contribute back. The decision is informed by hard data, not speculation.

The recommendation is now: **all of V2's Ambitious cohort + V2's deferred items, fired in parallel, with the L18 fork as a M12 go/no-go decision.**

### 2.5 What to buy vs build

At $30–50M/year burn with $50–100M raised, M&A becomes a real lever. Specific targets and rationale:

| Acquisition target | Estimated cost | What it brings | Strategic role |
|---|---:|---|---|
| **A small OSS-Iceberg-related project / team** (e.g., the maintainers of a niche Iceberg utility, or an early-stage Iceberg-focused company) | $5–20M | Iceberg domain expertise, community credibility, faster Track B execution | Accelerates Track B by 3–6 months; signals seriousness on Track III |
| **A small Trino-adjacent enterprise consultancy** | $3–10M | GTM expertise, existing enterprise relationships, integration know-how | Bootstraps Track G's enterprise sales motion |
| **2–3 acqui-hires (compiler / database-internals teams)** | $5–15M each | Senior engineering talent in scarce categories (CBO, JIT, query engines) | Fills the senior-engine and senior-distributed roles faster than recruiting |
| **A small visualisation / SQL-IDE OSS** (e.g., a stalled-but-promising browser SQL editor) | $1–5M | Track F (DX) ecosystem play; reduces the "I need a Snowflake-like notebook surface" gap | Track F accelerator |
| **A small cloud-billing / multi-tenant ops team** (often available as acqui-hires post-failed-startups) | $5–15M | Track G (cloud product) accelerator; multi-tenant + billing + auth + ops are well-understood but tedious | Saves Track G ~6 months of GA work |

**Total M&A budget recommendation: $25–50M over Year 1, separate from the $30–50M/year operating burn.** Most of this is acqui-hires; one or two strategic OSS-team acquisitions.

**Precedents.** Databricks acquired Tabular ($1B+, Iceberg origins) — set the precedent that table-format ownership is worth nine figures. Snowflake acquired Streamlit ($800M) — set the precedent that adjacent-tool ownership is worth nine figures. ClickHouse acquired ClickHouse-DBA-tools-companies for $5–20M each. The pattern is clear: at this scale, M&A is normal and expected.

**Buy-not-build decision rule.** Build if the work is in the core engine, the storage layer, or the learning loop (the three areas that constitute the moat). Buy/acqui-hire if the work is in well-understood adjacent territory (billing, ops, IDE, visualisation) where 6 months of internal build = 1 month of acquired team + integration.

---

## Part 3 — Organizational structure

### 3.1 The org chart at 50 engineers (M18 steady state)

```
CEO (founder)
├── VP Engineering ──── manages 4 Directors + Infra/SRE Director
│       │
│       ├── Director, Engine
│       │   ├── EM, Engine Core (Track A, 8–10 ICs)
│       │   └── EM, Storage (Track B, 5–6 ICs)
│       │
│       ├── Director, Distributed Systems
│       │   ├── EM, Distributed (Track C, 6–8 ICs)
│       │   └── EM, Adaptive Runtime (Track D, 4–5 ICs)
│       │
│       ├── Director, Product Engineering
│       │   ├── EM, Cloud Product (Track G, 8–10 ICs)
│       │   ├── EM, DX (Track F, 5–6 ICs)
│       │   └── EM, GPU (Track E, 3–4 ICs)
│       │
│       └── Director, Infrastructure
│           └── EM, Infra/SRE (Track I, 4–5 ICs)
│
├── VP Product ──── owns roadmap, product-market-fit interviews, design partners
│       └── Senior PM (cloud), Senior PM (engine), Senior PM (storage)
│
├── VP Sales (Year 2 hire) ──── owns Track G commercial GTM
│       └── (AEs, SEs, sales ops; ~10 people at Year 2)
│
├── Director, DevRel & Community ──── reports to CEO until Track G GA, then to VP Marketing
│       └── (Track H, 3–4 ICs + community managers)
│
├── VP Marketing (M15 hire) ──── owns positioning, content, conference, ematix.dev
│       └── (content lead, design lead, ops; ~5 people at Year 2)
│
├── CFO (M9 hire) ──── owns burn, runway, fundraise prep, M&A diligence
│       └── (controller, FP&A; ~3 people at Year 2)
│
├── General Counsel (M12 hire) ──── owns licensing strategy (BUSL vs Apache), M&A, employment
│
└── Head of People (M6 hire) ──── owns hiring sequence, culture, comp bands, mentorship structure
```

**Headcount summary by function:**
- Engineering: 50 (the 8 tracks above)
- Product: 4
- Sales (Year 2): 10
- Marketing + DevRel: 10
- G&A (Finance, Legal, HR): 6
- Executive (CEO, VP Eng, VP Product, VP Sales, VP Marketing, CFO, GC, Head of People): 8

**Total company: ~88 at full scale.** Engineering is 57% of headcount, which is healthy for a Series B/C deep-tech infrastructure company.

### 3.2 Decision rights and architectural board

At 50 engineers, **decision rights have to be explicit** or Conway's Law shapes the engine. The proposal:

- **CEO** owns the strategic frame (Part 1) and the M12 L18-fork go/no-go.
- **VP Engineering** owns the technical roadmap, headcount allocation across tracks, cross-team dependencies, and the architectural board.
- **Architectural board** (5–7 senior ICs + 4 EMs + VP Eng) meets weekly. Reviews all ADRs (Architecture Decision Records). Two-thirds vote required for cross-track architectural changes; veto power for the VP Eng on changes that compromise the moat (open Iceberg, OSS engine parity, learning loop).
- **Each Director** owns roadmap within their track, hiring within their track, performance management within their track, but **not** architectural decisions that span tracks.
- **EMs** own delivery, IC growth, and tactical roadmap execution.
- **ADRs are required for every cross-track decision and every API surface change.** Lives in `docs/decisions/NNNN-<title>.md`. One decision per ADR. At least two alternatives. The architectural board reviews the queue weekly.

This is the "Bezos one-way-door / two-way-door" framework applied to engineering: ADRs are required for one-way doors (decisions that are expensive to reverse — API surfaces, dependency choices, fork-or-not). Two-way doors (tactical implementation choices) don't need ADRs.

### 3.3 The first 6 hires beyond the existing team

The first 6 hires after the existing core team set the culture and the technical direction for the next 50 hires. They should be:

1. **VP Engineering** (executive). Sets the engineering culture, owns the org chart, hires the directors. Look for someone who has scaled from 5 to 50 before — ex-Databricks, ex-Confluent, ex-Snowflake-early, ex-Stripe-infra. **This is the most important non-CEO hire.**
2. **Senior database internals engineer** (Track A — engine core). Lead for L8 CBO + L13 custom join. Look for ex-DuckDB, ex-CockroachDB, ex-Snowflake-query-engine, ex-Vertica.
3. **Senior distributed systems engineer** (Track C — distributed). Lead for SF=100/SF=1000 publication. Look for ex-Trino, ex-Presto, ex-Spark-core, ex-Materialize-distributed.
4. **Senior storage / Iceberg engineer** (Track B — storage). Lead for L15 + sidecar Phase 2. Look for ex-Tabular, ex-Apache-Iceberg-contributor, ex-Snowflake-storage, ex-Hudi-core.
5. **Senior cloud-product engineer** (Track G — cloud product). Lead for the multi-tenant control plane + commercial product. Look for ex-Confluent-Cloud, ex-Mongo-Atlas, ex-Snowflake-control-plane, ex-Databricks-platform.
6. **Senior DevRel** (Track H — DevRel & community). Lead for conference circuit + Iceberg community engagement. Look for someone with existing brand in the data space — ex-Confluent-DevRel, ex-Databricks-DevRel, ex-DuckDB-community.

**Culture-shaping pattern.** These 6 hires set the bar: **deep technical seniority in the relevant domain**, willing to write code (not pure managers), comfortable with high-context async writing (ADRs, technical docs, RFCs). Avoid people who require warm-up time. The first 6 should each have a Day-1 measurable deliverable in their first 90 days.

### 3.4 Stepped hiring plan (5 → 15 → 25 → 50 over 18 months)

Pre-hiring is the failure mode every "5 to 50 in a year" company hits. The proposal is **stepped hiring gated on production proof points**:

| Phase | Headcount target | Cumulative | Calendar | Gate to unlock next step |
|---|---:|---:|---|---|
| **Phase 0** (current) | ~5 | 5 | M0 | — |
| **Phase 1** | +10 | 15 | M0–M3 | First 6 hires (above) onboarded; first track-EMs in seat; first PR from a non-founder hire shipped |
| **Phase 2** | +10 | 25 | M3–M9 | SF=100 bench harness running; first design partner customer signed; OSS GitHub stars > 2K (signals product-market interest) |
| **Phase 3** | +15 | 40 | M9–M15 | SF=100 bench *published*; first cloud GA preview; OSS GitHub stars > 5K; Track A L8 CBO in production; ARR > $1M |
| **Phase 4** | +10 | 50 | M15–M18 | Cloud GA shipped; ARR > $5M; the M12 L18 fork decision is made; Series B/C fundraise closed |

**The gates are real.** If Phase 2 doesn't hit its gate by M9, Phase 3 hiring pauses and the team re-evaluates strategy. The pre-mortem in Part 7 names "we pre-hired" as a failure mode.

**Comp bands at 50:**
- Senior IC: $250–350K total comp (base + equity + bonus, loaded ~$300K)
- Staff IC: $350–500K
- Principal IC: $500K+ (rare; 2–3 in the org)
- EM: $300–400K
- Director: $400–600K
- VP: $600K+ (likely with significant equity)

Loaded cost per engineer ~$300K → 50 engineers = $15M/year, matching Part 4's burn estimate.

### 3.5 Avoiding Conway's Law

50 engineers across 8 tracks risks producing 8 disconnected engines. Specific mitigations:

- **Quarterly all-engineering planning summits.** Every track presents its roadmap, dependencies, and risks. Cross-track surface conflicts surface here, not in Slack.
- **Mandatory cross-track rotation.** Each IC spends 1 quarter every 18 months on a different track. Builds the "I understand how this codebase fits together" muscle.
- **Single API surface review board.** Cross-track API changes (e.g., Track A's CBO output consumed by Track C's distributed planner) go through one review. Avoids the "two tracks evolved incompatible APIs in parallel" failure mode.
- **One shared monorepo with workspace-level dependency boundaries.** Sibling crates (`ematix-parquet`, `ematix-flow-planner`, `ematix-iceberg-sidecar`) own their own release cadence but live in the same repo. No micro-repo proliferation.

---

## Part 4 — Capital and business model

### 4.1 Burn breakdown at full scale (Year 2)

| Cost category | Annual cost | Notes |
|---|---:|---|
| **Headcount** (88 people loaded @ ~$300K avg, with ICs ~$300K and execs higher) | $24–28M | Eng @ 50 × $300K = $15M; G&A + sales + marketing + executive ≈ $10–13M |
| **Infrastructure** (cluster + GPU + bench hardware + observability + AWS/GCP) | $3–5M | SF=1000 bench is multi-day cluster runs; GPU instances are pricey; multi-tenant cloud staging |
| **Sales & marketing** (events, content, ads, agency) | $5–10M | re:Invent booth, Data+AI Summit, ematix-summit Y2, paid content, brand |
| **G&A** (legal, accounting, real estate if any, insurance, software) | $3–5M | Real-estate-light remote-first cuts ~$2M |
| **M&A** (acqui-hires + small acquisitions, see §2.5) | $25–50M one-time | Spread over Year 1 |
| **Total operating burn (excl. M&A)** | **$35–48M/year** | |

**Realistic burn: $35–45M/year.** $30M is achievable only by skipping the M&A path (which is suboptimal — see §2.5) or under-hiring the cloud team. $50M is achievable if M&A is aggressive in Year 1.

### 4.2 Implied funding raise

| Round | Amount | Implied valuation | Timing |
|---|---:|---:|---|
| **Series A (existing)** | $10–20M (assumed already in) | $50–100M | Pre-V3 |
| **Series B** | $50–80M | $250–400M | M6–M9 — gated on the first design-partner customers + SF=100 bench publication |
| **Series C** | $100–200M | $500M–$1B | M18–M24 — gated on $5–20M ARR + cloud GA + Iceberg moat established |

**Recent comparables:**
- ClickHouse $300M Series B → $2B valuation (Series B in late 2021 was the easiest year for raises; current market is harder)
- MotherDuck $100M Series B → $400M valuation (mid-2024; closer to current market)
- Starburst $250M Series D → $3.35B valuation (late-stage)
- Tabular $26M Series A → $200M valuation (pre-Databricks acquisition at $1B+)

ematix-flow at Series B should target **$50–80M raise at $250–400M valuation**, which is achievable IF:
1. The SF=100 published bench shows compelling cluster-scale numbers (15–50× DuckDB single-node, competitive with Trino on the same hardware).
2. There are 5–10 design-partner customers running pilots, with at least 2 paying.
3. The Iceberg moat narrative is credible (Track B has shipped sidecar Phase 2 + read-side, and the open table layer story is told via Track H DevRel).

If those proof points don't materialise by M9, the Series B raise stretches to M12 and the burn rate has to be cut. The Phase 3 hiring gate from §3.4 enforces this — we don't hire ahead of the proof.

### 4.3 Revenue model — when does the cloud product ship and at what tier?

**Year 1 (M0–M12):**
- M6: first design-partner pilots on a thin Cloud control plane. Unpaid or token paid ($5–20K pilot).
- M9: 3–5 design partners paying $20–50K/year for early-access tier.
- M12: first revenue: ~$300K–1M ARR from design partners + small commercial pilots.

**Year 2 (M13–M24):**
- M15: Cloud preview (closed beta) opens to a wider audience. Self-serve trial → paid conversion funnel begins.
- M18: Cloud GA. Pricing: $0.10–$0.50 per compute-hour per node (commodity-scale, designed to be 5–10× cheaper than Snowflake's per-credit pricing for equivalent workloads).
- M24: $5–20M ARR target. Mix of small self-serve customers ($5–50K/yr) and a handful of enterprise customers ($200K–$1M/yr).

**Year 3 (M25–M36):**
- Expansion + enterprise. $30–100M ARR target.

### 4.4 Open-core vs hosted vs BUSL vs dual-license — the licensing question

This is the most consequential commercial decision and deserves explicit framing.

| License model | Example | Pros | Cons |
|---|---|---|---|
| **Apache 2.0 OSS + proprietary hosted** (Cloud only) | Confluent, Snowflake/Streamlit, DataStax | OSS engine is permissive; hyperscaler-friendly; PLG funnel works | Hyperscalers can clone the OSS and resell — AWS/Aurora-Postgres pattern |
| **BUSL** (Business Source License — converts to OSS after N years) | MariaDB, CockroachDB, Sentry, Couchbase | Protects against hyperscaler resale during the commercial window | OSS community hostility; some users won't adopt non-Apache |
| **AGPL** | MongoDB historically, MinIO | Strong protection (server-side use triggers copyleft) | Enterprise IT often blocks AGPL by policy |
| **Dual-license (Apache OSS + commercial)** | MySQL historically, ScyllaDB | Best of both worlds in theory | Hard to delineate which features are which; OSS community feels the bait-and-switch |
| **SSPL** (Server Side Public License — MongoDB's invention) | MongoDB, Elastic | Strongest hyperscaler protection | OSI doesn't recognise as OSS; community pushback significant |

**Recommendation: Apache 2.0 OSS engine + proprietary ematix Cloud.** Reasoning:

1. **The Track III moat is the open storage layer.** Owning Iceberg-and-sidecar requires being trusted as an open-stack partner — BUSL/SSPL would actively hurt the Iceberg community engagement (Track H) and the de-facto-spec ambition.
2. **Hyperscaler-clone risk is real but manageable.** AWS has cloned Elasticsearch (OpenSearch), MongoDB (DocumentDB), and other OSS. The mitigation is that **the cloud product is the operations + integration + brand**, not the engine. Cloning the engine doesn't replicate the ematix Cloud experience. This is Confluent's pattern (Apache Kafka is hyperscaler-cloned; Confluent Cloud is the differentiator), and Confluent is a >$10B market cap public company. We follow that playbook.
3. **The OSS adoption story has to be honest.** Customers who trust ematix-flow OSS for production workloads need to know the OSS will keep being maintained at parity with Cloud. BUSL signals "we'll switch tracks when we hit revenue scale" — which kills enterprise adoption of OSS.

**The hedge if hyperscaler-clone risk materialises in Year 2:** trademark protection on "ematix" + commercial-tier feature differentiation (multi-tenant ops, billing integration, enterprise SSO, audit log, SLAs). The engine is open; the cloud's *operational maturity* is closed. AWS can clone the engine but cannot clone "we ran this engine in production for 50,000 multi-tenant customers and know all the failure modes."

### 4.5 The runway math

| Scenario | Burn | Series B raise | Runway from Series B close (M9) |
|---|---:|---:|---:|
| Optimistic | $30M/yr | $80M | 32 months |
| Realistic | $40M/yr | $60M | 18 months |
| Pessimistic (M&A-heavy + slower revenue) | $50M/yr | $50M (smaller raise due to weaker proof) | 12 months |

**Realistic runway: 18 months from Series B close.** Series C must close by M27 with $5–20M ARR + cloud GA + Iceberg moat established. The Phase 3 hiring gate at M9 is the company's commitment device — if the proof isn't there, we don't hire to scale, we stretch the runway.

---

## Part 5 — Competitive deep-dive

### 5.1 Direct competitors

#### DuckDB / MotherDuck

**What they do well.** DuckDB is the canonical single-binary embedded analytic engine. Mature SQL surface, great ergonomics, strong brand in the data-science community. MotherDuck is the hosted version targeting interactive analytics.

**Where they're vulnerable.** DuckDB is single-process by design. MotherDuck is "embedded with a cloud add-on" — not distributed. They have no story for SF≥100 cluster workloads, no story for petabyte data, no story for multi-region. They also don't own a table format (they consume parquet but don't write Iceberg natively at SF≥10K scale).

**Our differentiation.** ematix-flow at SF=10 single-node is approximately at-par with DuckDB after V2 Moderate cohort (geomean 0.58–0.62, V2 §3.2). At SF=100 cluster, we're 15–50× — they cannot follow us into that regime. The wedge is "scales from your laptop to a 100-node Arrow Flight mesh without a cluster service or a master node." DuckDB cannot say this. **Can MotherDuck absorb our positioning?** Only by inventing a distributed mode from scratch — 18+ months and a different architecture. They're more likely to stay in the "interactive single-user cloud DuckDB" lane and let us take the multi-node tier.

#### ClickHouse / ClickHouse Cloud

**What they do well.** ClickHouse is the canonical OLAP engine for time-series and high-cardinality analytical workloads. Fast scans, mature distributed mode (sharded), strong adoption in observability and ad-tech.

**Where they're vulnerable.** ClickHouse's distributed mode is sharded by design — joins across shards are painful (the famous "denormalize everything" story). Their SQL surface is non-standard (lots of ClickHouse-specific functions). Their storage format (`MergeTree`) is proprietary and not Iceberg-compatible. Their cloud product (ClickHouse Cloud) is well-executed but has lock-in.

**Our differentiation.** ematix-flow handles joins natively (Arrow Flight peer mesh, L13 custom hash join in Track A). Standard SQL via DataFusion + dialect translation. Open Iceberg storage — no proprietary format. The wedge is "Snowflake-tier perf at OSS-tier cost, on your own Iceberg tables, scales beyond a single shard." **Can ClickHouse absorb our positioning?** They could adopt Iceberg, but their `MergeTree` format is the bedrock of their perf story — switching is a multi-year rewrite. They're more likely to stay in time-series + high-cardinality lanes and let us take the general-analytical-workload tier.

#### Snowflake

**What they do well.** The category-leading cloud warehouse. Mature multi-tenant ops, strong sales motion, ecosystem (Snowpark, Snowflake Marketplace, Streamlit). Excellent customer success.

**Where they're vulnerable.** Pricing. Snowflake credits are expensive at scale — many customers report $1M+ annual bills for moderate workloads. The data is locked into Snowflake's proprietary format (FDB/micro-partitions); Iceberg support is recent and second-class. Self-hosted is not an option (it's cloud-only). Cost-conscious customers (mid-market, growth-stage) increasingly look for alternatives.

**Our differentiation.** ematix-flow targets the cost-sensitive 80% of workloads that Snowflake serves at a 10× premium. Self-hosted is a first-class option (OSS engine). Iceberg-native — no migration of data, just point ematix-flow at the customer's existing S3/GCS. **Can Snowflake absorb our positioning?** Partially — they can lower prices, but it conflicts with their margin model. They can improve Iceberg support, but their entire stack is optimised for the proprietary format. **The acquisition risk is real**: Snowflake could buy ematix-flow at $500M–$2B to remove the cost-perf-wedge competitor. We should plan for this (see Part 6 Year 3).

#### Databricks

**What they do well.** Spark-derived heritage, strong ML/AI positioning (especially post-MosaicML), enterprise-grade. Mature Delta Lake (their proprietary-ish table format, but with open spec). Acquired Tabular ($1B+) — owns Iceberg moat now.

**Where they're vulnerable.** Spark is the engine — Spark is JVM and slow for short queries. Photon (their C++ vectorised engine) helps but is proprietary and only available on Databricks. Cost is comparable to Snowflake. Delta-vs-Iceberg confusion (they now own both, sort of, but the strategic direction is unclear). Mid-market and self-hosted are not their sweet spot.

**Our differentiation.** Rust engine — faster than Spark+Photon on most analytical shapes, dramatically faster on short queries. Open Iceberg-only — no Delta confusion. Self-hosted + cloud. **Can Databricks absorb our positioning?** They tried — buying Tabular was the play to own Iceberg execution. But Tabular was the storage spec; **the execution layer is still open**. Databricks could build a Tabular-derived execution engine, but it would be Spark-based (slow) or net-new (3+ years). The acquisition risk is real here too — they might offer to buy us at $1B+ to consolidate the Iceberg execution layer.

#### Starburst (Trino-commercial)

**What they do well.** Enterprise-grade Trino, strong sales, federated query (one query across many sources). Established in the data-platform-team category.

**Where they're vulnerable.** Trino is JVM and slower than purpose-built engines. The federated-query positioning is becoming less compelling as data centralises into Iceberg lakes. Pricing is enterprise-tier; mid-market won't bite. Their engine performance roadmap is slow (Trino community moves slowly).

**Our differentiation.** Rust engine, 5–10× faster on TPC-H-shaped workloads. Self-hosted OSS is more competitive than Trino OSS (which is also Apache-licensed but operationally heavier). **Can Starburst absorb our positioning?** Only by rewriting Trino's engine, which they won't do. Most likely outcome: they slowly lose the perf-conscious tier of the market to us while keeping the federated-query niche.

#### Trino (OSS, Presto fork)

**What they do well.** Mature, multi-source, large community. Strong adoption in the enterprise data-platform-team category.

**Where they're vulnerable.** Same as Starburst — JVM, slow, enterprise-only-feeling, mid-market is uncomfortable with the operational complexity. The community moves slowly because too many production users gate every change.

**Our differentiation.** Same as vs Starburst. **Can Trino absorb our positioning?** No — it's a community project with no central commercial entity to drive a strategic pivot.

#### SingleStore

**What they do well.** HTAP — combines OLTP and OLAP in one engine. Strong adoption in real-time analytics + transactional-analytical workloads.

**Where they're vulnerable.** Proprietary format; cloud-only-ish; expensive. Not Iceberg-native. HTAP-focused workloads are a sub-category of analytics.

**Our differentiation.** OLAP-only (we don't do OLTP), but our OLAP is best-of-breed. Open storage. **Can SingleStore absorb our positioning?** Their HTAP focus is the right strategic lane for them; absorbing our positioning would dilute their HTAP story.

#### Apache Pinot

**What they do well.** Real-time analytics on time-series + event data. Strong adoption in LinkedIn, Uber, Stripe.

**Where they're vulnerable.** Narrow workload category (real-time-events). Not a general-purpose engine. Operational complexity.

**Our differentiation.** General-purpose. Open storage. **Absorb our positioning?** No — different lane.

#### MaterializeDB

**What they do well.** Streaming SQL with materialised views — incremental computation as data arrives. Strong technical execution (Frank McSherry team).

**Where they're vulnerable.** Streaming-SQL is a narrow category; their batch story is weak. Cloud-only.

**Our differentiation.** Batch-first; streaming via the existing Σ.D Kafka/Pub/Sub integrations + Arrow Flight, but not their differentiator. **Absorb our positioning?** No — different lane.

### 5.2 The wedge map (2x2)

```
                       What we OWN                         What we DON'T OWN
                       ┌─────────────────────────┐         ┌─────────────────────────┐
What we DO BETTER     │  Distributed OSS engine │         │  Snowflake-style multi- │
(perf, scale)         │  (vs Trino: faster)     │         │  tenant ops maturity     │
                      │                          │         │  (gap, year 2 close)     │
                      │  Iceberg execution      │         │                          │
                      │  layer (vs DuckDB:      │         │  Embedded SDK ergonomics │
                      │  distributed)            │         │  (DuckDB owns; we don't │
                      │                          │         │  enter)                  │
                      │  Learning optimiser     │         │                          │
                      │  (vs all: durable)      │         │                          │
                      └─────────────────────────┘         └─────────────────────────┘
                       ┌─────────────────────────┐         ┌─────────────────────────┐
What we DO COMPARABLY  │  SF=10 single-node TPC-H │         │  Spark/MLlib ecosystem  │
(parity, eventually)  │  vs DuckDB (M9 target:  │         │  (Databricks owns;       │
                      │  parity geomean)         │         │  not our wedge)          │
                      │                          │         │                          │
                      │  Standard SQL surface   │         │  Proprietary cloud DB    │
                      │  (DataFusion-derived;   │         │  (Snowflake owns; we    │
                      │  Postgres-compat goal)   │         │  attack from below)      │
                      └─────────────────────────┘         └─────────────────────────┘
```

**Reading the map.** Top-left is the differentiation zone — where we want to live. Top-right is the gap to close (cloud ops maturity in Year 2). Bottom-left is the table stakes — must be competitive but not the wedge. Bottom-right is the explicit non-goal (we don't enter MLlib, we don't enter embedded SDK, we don't enter proprietary cloud DB).

The single most important box: **owning the Iceberg execution layer**. That's the box that says "Databricks bought Tabular, but the execution-on-top is still open and we own it."

---

## Part 6 — Year-by-year milestones

### Year 1 (M0–M12): the proof-and-funding year

**Team size:** 5 → 25 (Phase 0 → Phase 1 → Phase 2 from §3.4).

**Engine (Track A):**
- M3: V2 Phase T1 outcomes shipped (L1 sidecar Phase 1, L3 PGO, L6). 22q SF=10 geomean ~0.70–0.72.
- M6: V2 Phase T2 + L13 kernel + L8 spike. Geomean ~0.65.
- M9: L8 CBO + L10 + L13 production. Geomean ~0.58. **All 6 V2-cited losses flipped to wins.** This is V2 Moderate cohort outcome at month 9.
- M12: L11 + L12 in progress. Geomean ~0.50–0.55. (V2 Ambitious cohort outcome.)

**Storage (Track B):**
- M3: Sidecar Phase 1 read-side in production (joint deliverable with Track A).
- M6: Sidecar Phase 2 (adaptive auto-creation, per `docs/plans/CURRENT.md`).
- M9: Iceberg read-side in production.
- M12: Iceberg write-side manifest generation. Sidecar generation tied to Iceberg write path.

**Distributed (Track C):**
- M3: SF=100 bench harness running on internal cluster.
- M6: First cluster runs at SF=100.
- M9: **SF=100 published bench — the dominant Year 1 strategic milestone.** Cluster runs at 15–50× DuckDB single-node, competitive with Trino. Published as a blog post + arXiv-style paper + presented at re:Invent / Data+AI Summit.
- M12: SF=1000 prep (hardware procurement, harness scaling).

**Adaptive (Track D):**
- M9: Σ.L.2 → L17 wire-up scaffolding (depends on Track A L8 shipping).
- M12: Learning loop in production for the first design-partner customers.

**DX (Track F):**
- M3: Python SDK polish + Web UI parity with DuckDB CLI feature surface.
- M6: ematix.dev re-launch + versioned docs.
- M9: Web UI parity with Snowflake/Databricks single-node surface (notebook, query history, schema browser, plan visualiser).
- M12: dbt-core adapter shipped; Mode/Hex connector shipped.

**Cloud (Track G):**
- M3: Cloud control plane design (no implementation yet — Track G ramping up).
- M6: First design partner pilots (3–5 customers, unpaid or token).
- M9: 3–5 design partners paying $20–50K/year early-access tier. **First revenue.**
- M12: Cloud preview opens to closed beta (50–100 customers on waitlist).

**Funding:**
- M0–M3: Series A close (assumed existing).
- M6: Series B prep begins.
- M9: **Series B close** ($50–80M at $250–400M valuation), gated on SF=100 publication + design-partner traction.
- M12: Phase 3 hiring (15 additional hires → 40 total) begins.

**The Year 1 board read:** "Shipped the SF=100 bench (the strategic proof). 5 design partners paying. Engine is at-par or below DuckDB on 22q SF=10. Cloud preview is in market. Series B closed. Track for Year 2 GA."

### Year 2 (M13–M24): the commercialisation year

**Team size:** 25 → 50 (Phase 3 → Phase 4).

**Engine (Track A):**
- M15: L11 (compile-time monomorphisation) in production. Geomean ~0.45.
- M18: L12 (zero-copy column pipeline) in production. String-heavy queries close their last residual.
- **M12 L18-fork decision:** if L8+L11+L12 hit DataFusion extensibility walls, decision is GO (fork DataFusion, Track A becomes a multi-year fork stewardship). If not, NO-GO (stay on DataFusion as upstream, contribute back).
- M21: GPU offload integration via Track E (production tier).
- M24: 22q SF=10 geomean ~0.40 — 2.5× DuckDB. SF=100 cluster geomean 30–80× DuckDB single-node.

**Storage (Track B):**
- M15: Partial materialised views + Z-order layouts. Σ.L.5 write-side tuner in production.
- M18: Iceberg write-side at full production quality. Sidecar Phase 2 fully adaptive.
- M21: Iceberg `attach_extension` (or equivalent open spec for sidecars) proposed to Apache Iceberg.
- M24: ematix-flow is the canonical Iceberg execution engine — referenced by Iceberg community, adopted by 2–3 other Iceberg consumers.

**Distributed (Track C):**
- M15: SF=1000 bench harness shipped.
- M18: SF=1000 published bench. **The "we scale beyond Snowflake" proof.**
- M21: Multi-tenant isolation + cluster auto-scaling in production.
- M24: Multi-region support.

**Adaptive (Track D):**
- M18: L17 production loop — the "engine that learns" wedge story has a published article + customer quote.
- M24: Σ.L.5 write-tuning loop closes for Iceberg + sidecar in production. "Your warehouse organises itself around your workload" is a real, demoable story.

**Cloud (Track G):**
- M15: Cloud preview → wider beta. Self-serve trial funnel opens.
- M18: **Cloud GA.** Pricing tiers published. Stripe integration live. SSO + audit log shipped.
- M21: $5M ARR.
- M24: **$5–20M ARR.** Mix of self-serve mid-market + 5–10 enterprise customers.

**Funding:**
- M18–M24: Series C prep.
- M27–M30: Series C close ($100–200M at $500M–$1B valuation), gated on ARR + Iceberg moat established.

**The Year 2 board read:** "Cloud GA. $5–20M ARR. SF=1000 published. Iceberg moat established with community recognition. Series C in flight or closed. Path to category leadership clear."

### Year 3 (M25–M36): category leadership or pivot

**The three forking outcomes:**

**Outcome A — category leadership.** ARR grows from $20M → $50–100M. Cloud customer count from 100 → 500+. The Iceberg execution layer is now the de-facto reference; Databricks-Tabular has been outflanked. Series C close at $500M–$1B+. Public IPO prep begins in Year 4. **This is the optimistic case.**

**Outcome B — strategic acquisition.** Snowflake or Databricks offers $1.5B–$3B to acquire ematix-flow + remove the cost-perf-wedge + lock down the Iceberg execution layer. The board decides whether to take it. Acceptable price band: **$2B+** (returns the Series C investors at 4–6×, leaves the founders well-positioned). Below $1.5B, decline and continue to Series D.

**Outcome C — pivot.** The Iceberg moat narrative didn't materialise — Databricks shipped a Tabular-derived execution engine and consolidated the layer. The cloud product is real but commoditised — Snowflake matched our pricing. The engine is best-of-breed but the wedge stories failed. **Pivot options:**
1. Vertical (e.g., become the analytical engine for fintech / healthcare / observability — narrow workload + premium pricing).
2. Embedded (re-enter Option C — fight MotherDuck for the embedded analytical engine).
3. Acquisition exit at a lower price band ($300M–$1B).

The pre-mortem in Part 7 names which failure modes lead to Outcome C.

---

## Part 7 — Risks and pre-mortem

It's 24 months from now. ematix-flow has failed (or significantly underperformed). What went wrong?

### Pre-mortem failure mode 1 — Conway's Law: 50 engineers couldn't ship coherent product

**Story.** The 8 tracks evolved 8 different APIs. The cloud product's billing layer didn't compose with the engine's metering. The distributed mode's planner had its own optimiser separate from Track A's CBO. Track B's storage layer assumed a different transaction model than Track G's cloud. By M18, integration debt was insurmountable; ship dates slipped 6+ months.

**Mitigation that would have helped.**
- The architectural board with ADRs and cross-track decision authority (§3.2).
- The single API surface review board (§3.5).
- Quarterly all-engineering planning summits with explicit cross-track dependency mapping (§2.2).
- Mandatory cross-track IC rotation (§3.5).

**Probability:** Medium. The mitigations work but only if executed disciplined; the failure mode is real for fast-growing teams.

### Pre-mortem failure mode 2 — Snowflake or Databricks copied the wedge faster than we shipped it

**Story.** At M12, Databricks shipped a Photon-derived "learning optimiser" demo and tied it to their Tabular acquisition. Snowflake announced "Snowflake Adaptive" with persistent-observer cross-run learning. The "engine that learns" wedge story evaporated because the incumbents could say the same thing and had the customer base to demonstrate it.

**Mitigation that would have helped.**
- Ship the Σ.L.2 → L17 production loop earlier — M9 instead of M12 (compress Track D's calendar).
- Get a customer to publicly attest "the engine improved 30% over 3 months on our workload" — concrete proof, hard to copy.
- Publish the technical architecture early (Track H DevRel) — make it harder for the incumbents to claim invention.
- Patent strategy (controversial — most OSS projects don't patent, but defensive patents in adaptive query optimisation are a hedge).

**Probability:** Medium-high. The incumbents have R&D resources to copy any single wedge; the mitigation is to ship more wedges than they can copy in parallel (Iceberg execution + cluster mode + learning + cost-perf).

### Pre-mortem failure mode 3 — the Σ.L learning loop turned out to be a curiosity, not a wedge

**Story.** Customers didn't notice or care about cross-run learning. The optimisations the loop produces are 5–15% wins, not the 5–50× the marketing materials implied. Conversations with prospects revealed they cared about predictability ("queries take the same time every time") more than adaptive improvement ("queries get faster over time").

**Mitigation that would have helped.**
- Customer research **before** committing Track D as a permanent team. Interview 20 prospects in the first 6 months; validate the wedge narrative.
- Quantify the learning loop's wins in the first design-partner pilots (M6–M9) and pivot the marketing if the numbers don't support the story.
- Have a fallback wedge ready — the open Iceberg execution layer is the structural moat even if the learning story underperforms.

**Probability:** Medium. The Σ.L substrate ships; the wedge story has to be validated with real customers, not assumed.

### Pre-mortem failure mode 4 — DuckDB / MotherDuck moved up-market with distributed mode

**Story.** At M15, MotherDuck announced "MotherDuck Cluster" — a 10-node Apache Arrow Flight peer mesh built on DuckDB. The "scales from laptop to cluster" wedge collapsed; they had the brand and the embedded user base, we had the architecture.

**Mitigation that would have helped.**
- Publish the SF=100 + SF=1000 cluster benches early (Track C M9 + M18) — establish ematix-flow as the cluster-scale incumbent before MotherDuck enters.
- Build a customer base on the cluster-scale workload (Track G — design partners with SF=100+ data) — switching cost protection.
- Differentiate on Iceberg+execution (Track B/D) — MotherDuck doesn't own table format; we do (jointly with the Iceberg community).

**Probability:** Medium. DuckDB's team is small but technically very strong; they could ship a distributed mode in 12–18 months. The mitigation is to be 2 years ahead, not 6 months ahead.

### Pre-mortem failure mode 5 — we burned 18 months on L18 (fork) and never shipped customer-visible features

**Story.** At M12, the team decided GO on the L18 DataFusion fork. By M24, the fork was 80% complete but not production-ready. Meanwhile, customer-visible features (cloud GA, new connectors, Web UI polish) lagged because engine engineers were absorbed by the fork. Cloud ARR underperformed. Competitors shipped while we re-architected.

**Mitigation that would have helped.**
- The M12 L18 go/no-go decision has explicit hurdle criteria: "fork only if L11+L12 cannot ship on upstream DataFusion within Q2 calendar." If L11+L12 can ship on upstream, NO-GO regardless of theoretical fork benefits.
- If GO, fork happens with a hard 6-month MVP gate. After 6 months, the fork must be on production parity or the project is abandoned (sunk-cost discipline).
- Keep a non-engine track of customer-visible work flowing in parallel (Track G + Track F never pause for engine internals).

**Probability:** High if the decision is made poorly. The mitigation is to make the decision well — V3 explicitly defers the decision to M12 with hard criteria.

### Pre-mortem failure mode 6 — Iceberg won as the table format, but the execution layer commoditised

**Story.** By M24, Iceberg was the dominant table format (predicted). But every analytical engine could read Iceberg — DuckDB shipped Iceberg support in M9, Snowflake's Iceberg support matured, Polars added it. Being "faster than Trino on Iceberg" stopped being a wedge because the alternative was "use Snowflake on Iceberg." The execution layer became a feature, not a product.

**Mitigation that would have helped.**
- Differentiate on **how** the engine uses Iceberg — Σ.L.5 write-side tuning (auto-organising tables for the workload), sidecar Phase 2 (auto-built indexes), partial materialised views — features that consume Iceberg differently than competitors.
- Push the open spec — `attach_extension`-style sidecars in the Iceberg spec — so we shape the ecosystem rather than just consuming it.
- Cloud product differentiation: ematix Cloud's ops maturity, not the engine itself, is the moat (the Confluent pattern).

**Probability:** Medium-high. Iceberg commoditisation is a real risk; the mitigation is to ride the commodity wave with cloud ops + adjacent open specs.

### Pre-mortem failure mode 7 — we pre-hired and the next 25 hires were the wrong people

**Story.** Phase 3 (M9) opened up 15 hires before the proof gates were fully met. Recruiters pushed for senior hires from FAANG who didn't understand database internals. The team grew to 40 by M15 but velocity dropped — onboarding overhead exceeded marginal-engineer output. By M18, cloud GA slipped because the new engineers were still ramping.

**Mitigation that would have helped.**
- Strict adherence to the Phase 2 → Phase 3 gate in §3.4. If the SF=100 bench hasn't shipped by M9, don't hire 15 people.
- Hiring rubric calibrated to database/distributed-systems domain expertise, not "senior engineer at FAANG."
- 90-day ramp targets for every new hire (a measurable deliverable, not just "onboarding").
- VP Eng owns velocity metrics and pauses hiring when ramp time exceeds output.

**Probability:** High. This is the most common failure mode for Series B/C engineering scaling. The mitigation is well-known but rarely executed.

### Pre-mortem failure mode 8 — the cloud product team underestimated multi-tenant ops complexity

**Story.** Track G shipped the cloud preview at M15. At M18 (GA), early customers hit billing issues, noisy-neighbour issues, multi-tenant data isolation issues. Customer success workload exploded. Engineering time was redirected to firefighting. Cloud ARR growth flatlined at $5M instead of $20M.

**Mitigation that would have helped.**
- Hire the senior cloud-product engineer in the first 6 hires (§3.3) — get the right experience early.
- Acqui-hire a small cloud-billing / multi-tenant ops team (§2.5) — buy the experience.
- Cloud preview at M15 is closed-beta with 10–20 customers, not 100 — let bugs surface at small scale.
- Defer cloud GA to M21 if M18 readiness review identifies risks — slipping is cheaper than firefighting.

**Probability:** Medium-high. Multi-tenant cloud is its own discipline; the mitigation requires explicit acknowledgement that engine excellence ≠ cloud-ops excellence.

### Pre-mortem failure mode 9 — Series B / Series C raise climate worsened and we couldn't fund the burn

**Story.** Macroeconomic conditions tightened in 2027. Series B markets for infra-software companies froze. ematix-flow's M9 Series B couldn't close at the target valuation; we settled for $40M at $200M valuation (down round vs Series A). By M18, burn was unsustainable. Layoffs at M21. Strategic acquisition at $400M instead of the optimistic Outcome A.

**Mitigation that would have helped.**
- Stretched Phase 3 hiring (don't burn the full $40M/year until Series B is locked).
- Cash discipline — keep 18 months of runway visible to the board.
- Plan B revenue path: if cloud GA slips, pivot to enterprise support contracts (Track A licensing revenue from large self-hosters). $5–10M ARR from support contracts is achievable even without GA.
- Multiple investor relationships, not just one lead. Stay close to 3–5 funds throughout the cycle.

**Probability:** Medium. Macro is outside our control; the mitigation is to be raise-ready with multiple paths.

### Pre-mortem failure mode 10 — the L18 fork decision was the wrong call (either direction)

**Story A — wrong to fork.** GO at M12; by M24, fork was 80% done, customer features lagged, competitors shipped. (Failure mode 5.)
**Story B — wrong to not fork.** NO-GO at M12; by M24, DataFusion upstream couldn't accept our extensions, our patches forked anyway de-facto, but without the rewrite cleanup we accumulated technical debt for years.

**Mitigation that would have helped.**
- M12 decision has both alternatives' criteria written explicitly in an ADR. If GO criteria meet but NO-GO criteria also meet, the default is NO-GO (sunk-cost-discipline default).
- DataFusion upstream relationship building from M0 — Track H + Track A both engage Apache PRC, contribute back, build trust. If we need to fork, we fork from a position of community alignment, not anger.

**Probability:** Low-medium. The decision is in 18 months; we have time to gather data. The risk is making the decision on intuition rather than evidence.

---

## Part 8 — Recommendation (the one-page board read)

### The one sentence

**Pursue Option B+D hybrid (commercial cloud product on top of an OSS distributed SQL engine, with the open Iceberg+sidecar storage layer as the structural moat); hire stepwise 5 → 15 → 25 → 50 over 18 months gated on production proof; raise Series B at M9 ($50–80M at $250–400M valuation) gated on SF=100 published bench and 5+ paying design partners; target $5–20M ARR by M24, Series C at $500M–$1B by M27.**

### What we build

- **Track A (engine, 8–10):** V2 Ambitious cohort + L18 fork as M12 go/no-go.
- **Track B (storage, 5–6):** Iceberg + sidecar Phase 2 + write-side tuning. **The moat.**
- **Track C (distributed, 6–8):** SF=100 published bench at M9. SF=1000 at M18. **The proof.**
- **Track D (adaptive, 4–5):** Σ.L learning loop in production. **The wedge story.**
- **Track G (cloud, 8–10):** Multi-tenant cloud product, design partners M6, GA M18. **The revenue.**
- **Tracks E (GPU), F (DX), H (DevRel), I (Infra):** support and accelerate.

### Who we hire

- First 6 (M0–M3): VP Eng, senior DB-internals engineer, senior distributed engineer, senior Iceberg engineer, senior cloud-product engineer, senior DevRel.
- Phases 1–4: 5 → 15 → 25 → 50 over 18 months, gated on production proof at each step (§3.4).
- M&A: $25–50M for acqui-hires (compiler/DB-internals teams, multi-tenant ops, billing) and one or two strategic OSS-team acquisitions (Iceberg-adjacent).

### How we sell

- **OSS engine** is the recruiting tool. Apache 2.0 license. Top-of-funnel via DevRel, conference talks, GitHub, ematix.dev.
- **ematix Cloud** is the revenue. Self-serve PLG ($0.10–$0.50/compute-hour-per-node, 5–10× cheaper than Snowflake equivalent) plus enterprise tier ($200K–$1M/year).
- **Iceberg moat narrative** is the differentiator vs Snowflake/Databricks: "your data stays in your S3, the engine is open, the cloud is the operational layer."

### What we differentiate on (the wedge map)

1. **Iceberg execution layer** — Tabular was acquired for the spec; we own the canonical execution engine on top.
2. **Cluster-scale that DuckDB/MotherDuck can't reach** — SF=100/SF=1000 published benches.
3. **Engine that learns from every query** — Σ.L production loop, persistent observer, customer-attested "got 30% faster over 3 months."
4. **5–10× cheaper than Snowflake on the same Iceberg tables** — the cost-perf wedge that attacks Snowflake's pricing model from below.

### What we tell the board at each milestone

- **M3:** Team scaled to 15. First 6 hires onboarded. V2 Phase T1 outcomes shipped. SF=100 harness running.
- **M9:** Team at 25. SF=100 published — strategic milestone hit. 5 design partners paying. Series B closing.
- **M12:** Team at 40. L18 go/no-go decision made (defensibly). Engine SF=10 geomean below DuckDB. Cloud preview in market.
- **M18:** Team at 50. Cloud GA. $5M ARR. Iceberg moat established with community recognition.
- **M24:** $5–20M ARR. SF=1000 published. Iceberg execution layer is the de-facto reference. Series C in flight.
- **M36:** Outcome A (category leadership, $50–100M ARR, IPO prep) or Outcome B (strategic acquisition at $2B+) or Outcome C (pivot).

### What we explicitly are NOT doing

- **Not entering the embedded SQL market.** MotherDuck owns it. Option C rejected.
- **Not adopting BUSL/SSPL.** The Iceberg moat requires open-stack trust. Apache 2.0 OSS + proprietary cloud.
- **Not pursuing Spark/MLlib ecosystem.** Databricks' lane. Different category.
- **Not pursuing OLTP/HTAP.** SingleStore's lane. Different category.
- **Not pre-hiring beyond the proof gates.** Phase 2 → Phase 3 → Phase 4 hiring is conditional.

### The risk we're betting on

The bet is that **owning the open Iceberg execution layer + a 10× cost-perf cloud product is the durable position between DuckDB (single-node embedded) below and Snowflake/Databricks (proprietary cloud) above**. The bet is wrong if (a) Iceberg execution commoditises faster than we can build the cloud product (mitigation: Track G ramp), (b) Databricks/Snowflake match the cost-perf without changing their business model (unlikely — it conflicts with their margin), or (c) the OSS engine fails to attract a community large enough to feed the cloud funnel (mitigation: Track H DevRel).

The bet is correct if Iceberg becomes the open Linux of analytic storage, and ematix-flow becomes the canonical execution layer that runs Iceberg best — at which point ematix Cloud is the natural commercial expression of that position.

---

## References

- V1 (`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE.md`) — per-query root cause, lever menu L1–L7, codegen-tax-constrained sequencing.
- V2 (`docs/PHASE_SIGMA_T_SF10_WEAKNESS_CLOSURE_V2.md`) — full L1–L18 menu, Conservative/Moderate/Ambitious cohorts, strategic discussion (§5) that V3 picks up.
- Sidecar plan (`docs/plans/CURRENT.md`) — Phase 1 read-side + Phase 2 adaptive, the substrate for Track B.
- `ematix.dev/concepts/why-ematix-flow.mdx` — current wedge positioning (8 numbered claims; V3 §1.3 and §5 reframe).
- `ematix.dev/reference/whats-shipped.mdx` — current capability surface (V3 baseline for what's already done vs what's roadmap).
- Memory `project_distributed_is_shipped.md` — distributed batch SQL is shipped; the SF=100 publication is the dominant Year 1 milestone.
- Memory `project_sigma_l_adaptive_runtime.md` — Σ.L 32-test pipeline; foundation for Track D + L17.
- Memory `project_sigma_q_l13_to_l16_session.md` — current 22q SF=10 geomean 0.80 baseline; V3 Track A M9 target 0.58, M24 target 0.40.
- Memory `project_optimizer_codegen_sensitivity.md` — codegen tax; Track I PGO infra + sibling-crate discipline is the mitigation at scale.
- Memory `project_ematix_parquet_repo.md` — sibling-crate model; template for Track B's `ematix-iceberg-sidecar` and Track A's `ematix-flow-planner`.
- Memory `feedback_fewer_prs.md`, `feedback_recommend_next_step.md`, `feedback_tdd.md` — team-discipline patterns preserved at scale.
- Recent comparables for funding rounds and acquisitions: ClickHouse $300M Series B, MotherDuck $100M Series B, Starburst $250M Series D, Databricks-Tabular $1B+, Snowflake-Streamlit $800M.

---

*End of V3. The V3 doc takes positions where V2 laid out options. The reader walks away knowing what to tell their board: Option B+D hybrid, 50 engineers, $40M/year burn, $60M Series B at M9, $5–20M ARR by M24, the Iceberg execution layer is the moat, and the L18 fork decision is made on data at M12. If any of those positions are wrong, the pre-mortem in Part 7 names which failure mode and what mitigation should have been put in place — making the decision auditable rather than aspirational.*
