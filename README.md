# Legacy-to-Fabric Migration: OULAD Student Analytics

A production-style migration of a legacy SQL Server / SSIS reporting workload into a
**Microsoft Fabric** Lakehouse, using a Bronze/Silver/Gold **medallion architecture**,
a **Kimball star schema**, a governed **Direct Lake** semantic model, and a multi-page
Power BI report — orchestrated end-to-end and secured with row- and column-level security.

Built on the **Open University Learning Analytics Dataset (OULAD)** — 28,785 students,
32,593 registrations, and a 10.7M-row engagement clickstream.

<!-- TODO: architecture diagram image will be here -->
<!-- TODO: report screenshots and video walkthrough link will be here -->



## Why this project

Legacy BI stacks built on SQL Server + SSIS tend to share the same pain: tight coupling,
no data lineage, no version control, and manual reprocessing. This project re-architects
that pattern into a governed, reproducible Fabric Lakehouse — and validates the migration
end-to-end with row-count reconciliation at every layer.

The goal was fidelity, not just a demo: a real legacy warehouse stands in as the source,
so the "SQL Server -> Fabric" path is literal and reconcilable, with a monolithic legacy
T-SQL transform preserved as the documented "before" baseline.



## Architecture

```
Legacy SQL Warehouse  ->  Bronze  ->  Silver  ->  Gold  ->  Direct Lake Semantic Model  ->  Power BI
   (source of truth)      (raw)      (clean)    (star)        (no refresh, live)             (report)
```

The full pipeline is orchestrated as a scheduled Fabric Data Pipeline:
**Copy -> Silver notebook -> Gold notebook -> semantic model refresh.**

- **Bronze** — raw ingestion via Copy activity, preserved as-is with `_ingested_at` audit lineage.
- **Silver** — typed and cleaned in PySpark: imputation, standardization, derived flags, a
  validation/quarantine framework, and clickstream aggregation (10.7M -> 8.5M rows, validated lossless).
- **Gold** — Kimball star schema: 3 dimensions, 3 fact tables, surrogate keys; the high-volume
  engagement fact is tuned with Delta OPTIMIZE and V-Order.
- **Semantic model** — Direct Lake over Gold; star relationships; 12 DAX measures; row- and
  column-level security enforced via Entra ID.
- **Report** — four-page Power BI report (Overview, Registrations & Outcomes, Demographics
  & Engagement, Assessments & Performance).



## Data model (Gold)

**Dimensions**
- `dim_student` (28,785) — one row per student
- `dim_module` (22) — course offerings
- `dim_assessment` (206) — assessments

**Facts**
- `fact_assessment` (173,912) — assessment scores
- `fact_vle` (8,459,320) — aggregated engagement (clicks per student-material-day)
- `fact_registration` (32,593) — enrollment outcomes incl. withdrawal

`dim_student` is a conformed dimension feeding all three facts.



## Key engineering decisions

- **Re-architect, not lift-and-shift** — the medallion pattern directly addresses the legacy
  stack's coupling, lineage, and reprocessing problems.
- **Reconciliation at every layer** — source baselines captured first, every layer validated
  against them (e.g. `sum(event_count) = 10,655,280` proves the clickstream aggregation was lossless).
- **Quarantine over drop** — invalid records are routed to a quarantine table, never silently dropped.
- **SCD2 evaluated, then dropped** — OULAD records attributes per-registration with no temporal
  change timeline, so Type 2 versioning wasn't meaningful here. Built a clean one-row-per-student
  dimension and documented SCD2 as the design for a source with real temporal change data.
- **Surrogate keys + skinny facts** — facts carry keys and measures only; descriptive attributes
  live in dimensions. Fan-out bugs were caught via row-count reconciliation after each join.
- **Performance tuning** — the 8.5M-row engagement fact is compacted with Delta OPTIMIZE and
  V-Order for Direct Lake read performance.



## Governance

- **Row-level security (RLS)** and **column-level security (CLS)** enforced through the semantic
  model via Entra ID, scoping data access by role.
- **Assume referential integrity** enabled on fact-to-dimension relationships, validated by
  zero-orphan key checks, for faster Direct Lake joins.



## Headline findings

- **~31% of registrations end in withdrawal** — the dataset's primary attrition signal.
- **Lower prior education strongly predicts withdrawal** — 43% (no formal quals) vs. 23% (postgraduate).
- **Coursework outscores exams** (CMA/TMA higher than final exams).
- **Attrition is via withdrawal, not failure** — pass rates run 89-98%, so the risk is students
  leaving, not failing.



## Tech stack

Microsoft Fabric · OneLake · Delta Lake · PySpark · Spark SQL · T-SQL · Kimball dimensional
modeling · Direct Lake · Power BI · DAX · Entra ID (RLS/CLS) · Fabric Data Pipelines · Git integration



## Repository structure

```
legacy-to-fabric-migration/
├── docs/                  # diagnostics, design notes, migration strategy, legacy baseline
├── notebooks/             # Bronze / Silver / Gold transforms (Fabric-serialized)
├── legacy/                # monolithic legacy T-SQL transform (the "before" baseline)
├── pipelines/             # orchestration pipeline definition
└── README.md
```



## Documentation

- `docs/migration-strategy.md` — as-is / to-be architecture, table mapping, cutover and
  reconciliation approach.
- `docs/legacy-baseline.md` — the legacy system's structure, type debt, and data-quality gaps.
- `docs/diagnostics.sql` — source data inspection with findings and decisions.
- `docs/interview-notes.md` — engineering decisions and rationale captured during the build.



## Dataset

Open University Learning Analytics Dataset (OULAD) — Kuzilek, J., Hlosta, M., & Zdrahal, Z. (2017).
Publicly available for research use.
