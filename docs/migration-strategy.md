# Migration Strategy: Legacy SQL Server / SSIS to Microsoft Fabric

How a coupled, single-pass legacy reporting workload was re-architected into a governed, layered Microsoft Fabric Lakehouse, and how that migration would be validated before cutover.
Dataset: Open University Learning Analytics Dataset (OULAD), 28,785 students, 32,593 registrations, 10.7M-row engagement clickstream, modeled as a legacy SQL Server warehouse and migrated end to end.


## 1. As-Is  the legacy state

The legacy reporting workload is a single SQL warehouse (`legacy_dw`) holding seven raw tables, transformed by one monolithic T-SQL script (`legacy/monolithic_transform.sql`) that produces a wide denormalized reporting table in a single pass.
What it does and why it is limiting:

- **No layering.** Raw tables go straight to a final reporting table in one query, no staging, no intermediate checkpoints. There is nowhere to inspect, validate, or reprocess a middle step.
- **Inline everything.** Type casting, null handling, and business logic are tangled together inside one SELECT. Changing one rule means re-reading the whole statement.
- **Mixed grain.** Per-registration rows carry per-student aggregates, so values are silently duplicated across a student's registrations.
- **No reconciliation, lineage, or idempotency.** The script drops and recreates its output with no row-count validation and no audit trail. A re-run cannot be reasoned about or verified.
- **Correlated subqueries** re-scan the 10.7M-row clickstream per student rather than aggregating once, an avoidable, row-by-row performance pattern.

The result is a transform that is hard to maintain, impossible to validate incrementally, and slow, the typical pain of a coupled legacy stack.



## 2. To-Be the Fabric target

The workload is re-architected into a Bronze / Silver / Gold medallion Lakehouse, served by a Direct Lake semantic model.

| Layer | Purpose | Built with |
| --- | --- | --- |
| **Bronze** | Raw, immutable, replayable ingestion with audit lineage (`_ingested_at`) | Copy activity to Delta |
| **Silver** | Typed, cleaned, conformed; validation/quarantine framework; clickstream aggregated 10.7M to 8.5M (lossless) | PySpark notebook |
| **Gold** | Kimball star schema: 3 dimensions, 3 fact tables, surrogate keys | PySpark notebook |
| **Semantic** | Direct Lake model over Gold; 12 DAX measures; star relationships | Power BI |
| **Governance** | Dynamic row-level security via Entra ID identity + mapping table | Semantic model RLS |
| **Orchestration** | Silver to Gold to semantic refresh, chained on success, scheduled | Fabric Data Pipeline |
| **Performance** | Delta OPTIMIZE compaction + V-Order on the high-volume fact | Spark / Delta |

Each layer exists for a reason the legacy stack could not satisfy: Bronze buys replayability and auditability; Silver isolates cleaning and quarantines bad records instead of dropping them; Gold serves analytics at proper grain with surrogate keys; Direct Lake gives import-speed reads on live data with no refresh.



## 3. Legacy to Fabric mapping

| Legacy (SQL Server / SSIS) | Fabric equivalent |
| --- | --- |
| SQL Server staging tables | Bronze Lakehouse (raw Delta) |
| SSIS data flow (extract/move) | Data Pipeline Copy activity |
| Stored-proc / monolithic T-SQL transform | Silver + Gold PySpark notebooks (layered) |
| Inline cleaning in the transform | Silver: typed casts, imputation, quarantine framework |
| Denormalized reporting table | Gold: Kimball star schema (dims + facts, surrogate keys) |
| SSAS cube | Direct Lake semantic model |
| SQL Agent schedule | Data Pipeline schedule |
| File-share / manual versioning | Git integration (workspace ↔ repo) |
| Ad-hoc / no row validation | Reconciliation at every layer (baseline-then-validate) |


## 4. Migration approach re-architect, not lift-and-shift

The migration deliberately **re-architects** into a medallion Lakehouse rather than lifting-and-shifting the monolithic transform as-is.
A lift-and-shift would have preserved the legacy stack's core problems, no layering, no lineage, no reproducibility, mixed grain, just on newer infrastructure. The pain was architectural, not platform-specific, so moving the same single-pass logic to Fabric would have moved the same liabilities with it.
Re-architecting addresses each root cause directly: layering restores inspectability and incremental reprocessing; separating ingestion from transformation buys replayability; a dimensional Gold layer fixes the grain problem the monolith papered over; and set-based Spark aggregation replaces the per-row correlated subqueries. The cost is more upfront design and more moving parts, justified here because the workload is meant to be maintained, governed, and extended, not run once.
A lift-and-shift would be the right call only for a workload being retired soon, or under a hard deadline where modernization could follow later. This one is the opposite case.



## 5. Risk & validation  cutover

The migration is de-risked by running legacy and Fabric in parallel and reconciling before any cutover.

- **Parallel run.** Keep `legacy_dw` and the Fabric pipeline running side by side during transition; neither is decommissioned until reconciliation passes.
- **Reconciliation at every layer.** Source row counts are captured as baselines first, then every layer is validated against them. Examples from this build: Bronze reconciled lossless across all 7 tables; the clickstream aggregationproved lossless via `sum(event_count) = 10,655,280`; fact-to-dimension joins were validated with zero null surrogate keys.
- **Anti-join checks.** Rows present in one system but not the other are surfaced with anti-joins (`LEFT JOIN … WHERE key IS NULL` / `NOT EXISTS` / `EXCEPT`) in both directions, the tool for proving two outputs are identical.
- **Phased cutover + rollback.** Move reporting to the Fabric model once totals reconcile; retain the legacy path as rollback until the new platform is trusted in production.



## 6. Outcome

A reporting workload that was coupled, single-pass, unversioned, and slow is now layered, reconciled at every stage, version-controlled, governed with row-level security, orchestrated on a schedule, and served live through Direct Lake.
Key figures: 10.7M to 8.5M clickstream aggregation (lossless) · ~31% withdrawal rate surfaced · 0 orphan keys across fact-to-dimension joins · OPTIMIZE compaction on the engagement fact · 28,785 students / 32,593 registrations / 173,912 assessments modeled.