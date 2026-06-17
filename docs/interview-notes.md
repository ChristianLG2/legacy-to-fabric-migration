# Interview Notes — Legacy-to-Fabric Migration

Running capture of interview-relevant decisions, lessons, and metrics from the
build. Organized by theme. To be shaped into a full prep doc (with 2-min and
10-min project walkthroughs) once the project is complete.

---

## Architecture & migration decisions

- **Re-architect vs. lift-and-shift:** I deliberately re-architected to a medallion
  Lakehouse rather than lifting-and-shifting, because the legacy pain was coupling,
  no lineage, no version control, and manual reprocessing — medallion solves each.
- **Real legacy warehouse (Path B):** I stood up an actual SQL warehouse as the
  legacy source and wrote the monolithic transform, so the SQL Server → Fabric
  migration is literal, not described — which lets me show a real before/after for
  reconciliation.
- **Copy vs. notebook for Bronze:** Bronze ingestion is movement, so I used a Copy
  activity (the modern SSIS data flow); transformation logic lives in Spark from
  Silver onward.
- **Lakehouse vs. Warehouse:** A Warehouse gives full read-write T-SQL — needed for
  the legacy transform; a Lakehouse is Spark-first with a read-only SQL analytics
  endpoint. Used the right one for each job.
- **Dual-access Lakehouse:** A Lakehouse exposes both a Spark surface and a
  read-only SQL endpoint over the same Delta tables — analysts can SQL the lake
  without touching Spark.
- **Direct Lake (later):** the modern SSAS-cube replacement — import-like speed,
  live data, no scheduled refresh.

## Ingestion & idempotency

- **Full vs. incremental:** Full copy for a one-time/static load; incremental with a
  watermark for ongoing production ingestion where only changed rows should move.
- **Overwrite vs. append/merge:** Overwrite for full-reload idempotency — reruns
  can't duplicate. In production with growing data I'd switch to incremental merge
  to avoid reprocessing full history every run.
- **Ingestion lineage:** Bronze stamps `_ingested_at` (load time) per row; overwrite
  refreshes it each run.
- **Audit-column judgment:** kept the dynamic timestamp, dropped a static
  `_source_table` because the Bronze table name already encodes the source —
  source-lineage columns earn their place only when multiple sources merge into one
  table.

## SQL / set-based thinking (flagged gaps)

- **Anti-join pattern:** "Rows in A not in B" = LEFT JOIN + `WHERE right_key IS NULL`.
  The NULL is the fingerprint of a failed match; anti-join always tests the key column.
- **Filter placement:** match conditions go in `ON`; post-match filters go in `WHERE`
  (the anti-join null check is a `WHERE`).
- **Why a single count hides a discrepancy:** an INNER JOIN can't find what's missing,
  and a count equal to expectation doesn't prove a clean match — must anti-join *both
  directions* to claim two tables are identical.
- **Composite key reasoning:** the key is the *minimum* columns that uniquely identify
  one row. `code_module + code_presentation` = a course offering (thousands of rows);
  add `id_student` to reach one enrollment. Validate with
  `GROUP BY key HAVING COUNT(*) > 1` (zero rows = valid key).
- **UNION ALL vs UNION:** `UNION ALL` skips the dedup/sort cost; use it unless you
  specifically need de-duplication.
- **Integer-division trap:** `int / int` truncates in T-SQL — force a decimal
  (`* 100.0` or `CAST`) before dividing for rates/percentages. Sanity check:
  percentages should sum to ~100.
- **Loop over tables, never over rows:** looping a list of tables/files
  (metadata-driven pipelines) is the mature pattern; looping over data rows
  (`.collect()` then per-row) destroys Spark's parallelism — express it set-based.

## PySpark / Spark engine

- **Lazy evaluation:** transformations build a plan; nothing runs until an *action*
  (`.count()`, `.show()`, write). That's how Spark optimizes the whole DAG.
- **PySpark is a Python library, not a language:** "I write Python that orchestrates
  distributed Spark operations; the engine is JVM under the hood, PySpark is the
  Python API."
- **No SparkSession boilerplate in Fabric** — `spark` is pre-attached.
- **Cast by string vs. type object:** `.cast("integer")` and `.cast(IntegerType())`
  are equivalent; type objects matter for explicit schemas.
- **DataFrames are immutable:** `withColumn` returns a new DataFrame; reassign it.
- **Read errors precisely:** `AnalysisException` tells you exactly what's wrong
  (unresolved column → check the "did you mean" suggestion; the Explorer tree is the
  source of truth for the schema-qualified path like `bronze.dbo.studentInfo`).

## Data quality & modeling discipline

- **Inspect before you transform:** every diagnostic answers "what will I fix in
  Silver, and how do I justify it?" Quantify dirtiness before writing a rule.
- **Name ≠ type:** `date_unregistration` *sounds* like a date but is a day-offset
  integer; `date` in assessments too. Always look at actual values before choosing a
  type. (Caught a real bug.)
- **Check distinct values before choosing a numeric type:** `weight` had `7.5` →
  `double`, not `integer` (integer would silently truncate).
- **Defensive empty checks:** test `IS NULL OR = ''` on CSV-loaded columns — missing
  values land as NULL *or* empty string depending on the column.
- **Flag before cast:** derive flags from the raw column before casting, since casting
  changes what "empty" looks like.
- **Transformation order:** order matters only when one rule's *output* feeds
  another's *input* — otherwise independent.
- **Keep, don't drop:** ~30% of registrations were withdrawals → flag with
  `is_withdrawn`, never drop (would erase the most analytically interesting cohort —
  retention analysis).
- **Type consistency across tables:** keeping `id_student` as string everywhere
  prevents silent string-vs-int join failures.
- **Dedup vs. aggregate judgment:** for additive clickstream data, `row_number()`
  dedup is lossy — chose `groupBy().agg(sum)` to preserve clicks, plus an
  `event_count` column to measure collapse.

## Performance (JD-named)

- **Data skew (observed live):** "When I aggregated the 10.6M-row clickstream, Spark
  flagged mild skew — some students generate far more activity, so their shuffle
  partitions were heavier. Negligible at this scale; at production scale you'd salt
  the key or rely on adaptive query execution."
- **Window-function dedup pattern (to drill separately):** partition by key, order to
  pick the winner, `row_number().over(w)`, filter `rn == 1`. Didn't fit studentVle
  (chose aggregation) — practice it on re-registration in Gold.

## Reconciliation (strongest theme)

- **Baseline-then-validate:** captured source row counts first, then validated every
  layer against them. Bronze reconciled lossless across all 7 tables.
- **Aggregation reconciliation (the elegant one):** `sum(event_count)` = 10,655,280
  proves aggregation collapsed duplicates without losing rows.

## Quotable metrics

- 10.66M raw clickstream events → 8.46M aggregated daily records (lossless, reconciled).
- ~30% of registrations are withdrawals (kept + flagged).
- 3.4% of students missing `imd_band` (imputed, not dropped).
- 32,593 registrations across 28,785 distinct students (re-registration → composite
  key, SCD2 candidate).

---

_Still to come: quarantine framework (studentAssessment), Gold dimensional model
(SCD2 decision, surrogate keys, fact grain), Direct Lake semantic model, governance
(RLS/CLS), orchestration pipeline, performance tuning (OPTIMIZE/V-Order)._