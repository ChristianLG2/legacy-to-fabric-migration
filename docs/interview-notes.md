# Interview Notes: Legacy-to-Fabric Migration

Running capture of interview-relevant decisions, lessons, and metrics from the build. Organized by topic.

---

## Architecture & migration decisions

- **Re-architect vs. lift-and-shift:** I deliberately re-architected to a medallion Lakehouse rather than lifting-and-shifting, because the legacy pain was coupling, no lineage, no version control, and manual reprocessing medallion solves each.
- **Real legacy warehouse (Path B):** I stood up an actual SQL warehouse as the legacy source and wrote the monolithic transform, so the SQL Server to Fabric migration is literal, not described which lets me show a real before/after for reconciliation.
- **Copy vs. notebook for Bronze:** Bronze ingestion is movement, so I used a Copy activity (the modern SSIS data flow); transformation logic lives in Spark from Silver onward.
- **Lakehouse vs. Warehouse:** A Warehouse gives full read-write T-SQL needed for the legacy transform; a Lakehouse is Spark-first with a read-only SQL analytics endpoint. Used the right one for each job.
- **Dual-access Lakehouse:** A Lakehouse exposes both a Spark surface and a read-only SQL endpoint over the same Delta tables, analysts can SQL the lake without touching Spark.
- **Direct Lake (later):** the modern SSAS-cube replacement, import-like speed, live data, no scheduled refresh.

## Ingestion & idempotency

- **Full vs. incremental:** Full copy for a one-time/static load; incremental with a watermark for ongoing production ingestion where only changed rows should move.
- **Overwrite vs. append/merge:** Overwrite for full-reload idempotency, reruns can't duplicate. In production with growing data I'd switch to incremental merge to avoid reprocessing full history every run.
- **Ingestion lineage:** Bronze stamps `_ingested_at` (load time) per row; overwrite refreshes it each run.
- **Audit-column judgment:** kept the dynamic timestamp, dropped a static `_source_table` because the Bronze table name already encodes the source, source-lineage columns earn their place only when multiple sources merge into one table.

## SQL / set-based thinking (flagged gaps)

- **Anti-join pattern:** "Rows in A not in B" = LEFT JOIN + `WHERE right_key IS NULL`. The NULL is the fingerprint of a failed match; anti-join always tests the key column.
- **Filter placement:** match conditions go in `ON`; post-match filters go in `WHERE` (the anti-join null check is a `WHERE`).
- **Why a single count hides a discrepancy:** an INNER JOIN can't find what's missing, and a count equal to expectation doesn't prove a clean match, must anti-join *both directions* to claim two tables are identical.
- **Composite key reasoning:** the key is the *minimum* columns that uniquely identify one row. `code_module + code_presentation` = a course offering (thousands of rows); add `id_student` to reach one enrollment. Validate with `GROUP BY key HAVING COUNT(*) > 1` (zero rows = valid key).
- **UNION ALL vs UNION:** `UNION ALL` skips the dedup/sort cost; use it unless you specifically need de-duplication.
- **Integer-division trap:** `int / int` truncates in T-SQL, force a decimal (`* 100.0` or `CAST`) before dividing for rates/percentages. Sanity check: percentages should sum to ~100.
- **Loop over tables, never over rows:** looping a list of tables/files (metadata-driven pipelines) is the mature pattern; looping over data rows (`.collect()` then per-row) destroys Spark's parallelism, express it set-based.

## PySpark / Spark engine

- **Lazy evaluation:** transformations build a plan; nothing runs until an *action* (`.count()`, `.show()`, write). That's how Spark optimizes the whole DAG.
- **PySpark is a Python library, not a language:** "I write Python that orchestrates distributed Spark operations; the engine is JVM under the hood, PySpark is the Python API."
- **No SparkSession boilerplate in Fabric**, `spark` is pre-attached.
- **Cast by string vs. type object:** `.cast("integer")` and `.cast(IntegerType())` are equivalent; type objects matter for explicit schemas.
- **DataFrames are immutable:** `withColumn` returns a new DataFrame; reassign it.
- **Read errors precisely:** `AnalysisException` tells you exactly what's wrong (unresolved column -> check the "did you mean" suggestion; the Explorer tree is the source of truth for the schema-qualified path like `bronze.dbo.studentInfo`).

## Data quality & modeling discipline

- **Inspect before you transform:** every diagnostic answers "what will I fix in Silver, and how do I justify it?" Quantify dirtiness before writing a rule.
- **Name ≠ type:** `date_unregistration` *sounds* like a date but is a day-offset integer; `date` in assessments too. Always look at actual values before choosing a type. (Caught a real bug.)
- **Check distinct values before choosing a numeric type:** `weight` had `7.5` to `double`, not `integer` (integer would silently truncate).
- **Defensive empty checks:** test `IS NULL OR = ''` on CSV-loaded columns, missing values land as NULL *or* empty string depending on the column.
- **Flag before cast:** derive flags from the raw column before casting, since casting changes what "empty" looks like.
- **Transformation order:** order matters only when one rule's *output* feeds another's *input*, otherwise independent.
- **Keep, don't drop:** ~31% of registrations were withdrawals -> flag with `is_withdrawn`, never drop (would erase the most analytically interesting cohort, retention analysis).
- **Type consistency across tables:** keeping `id_student` as string everywhere prevents silent string-vs-int join failures.
- **Dedup vs. aggregate judgment:** for additive clickstream data, `row_number()` dedup is lossy, chose `groupBy().agg(sum)` to preserve clicks, plus an `event_count` column to measure collapse.

## Performance (JD-named)

- **Data skew (observed live):** "When I aggregated the 10.6M-row clickstream, Spark flagged mild skew, some students generate far more activity, so their shuffle partitions were heavier. Negligible at this scale; at production scale you'd salt the key or rely on adaptive query execution."
- **Window-function dedup pattern:** partition by key, order to pick the winner, `row_number().over(w)`, filter `rn == 1`. Used for `dim_student` in Gold; didn't fit `studentVle` (chose aggregation instead).

## Reconciliation (strongest theme)

- **Baseline-then-validate:** captured source row counts first, then validated every layer against them. Bronze reconciled lossless across all 7 tables.
- **Aggregation reconciliation (the elegant one):** `sum(event_count)` = 10,655,280 proves aggregation collapsed duplicates without losing rows.

---

## Quarantine & validation framework

- **Quarantine over drop (the maturity signal):** invalid `studentAssessment` scores are routed to a quarantine table, not silently dropped, valid rows (score 0–100) flow to Silver, out-of-range rows are isolated for review. Drop loses evidence; quarantine preserves it and makes the DQ rule auditable.
- **The null-in-both-filters trap:** a naive "valid" filter (`score BETWEEN 0 AND 100`) and "invalid" filter (`score < 0 OR score > 100`) *both* silently exclude NULLs, so nulls vanish from both outputs and the counts don't reconcile to the source. Fix: handle NULL explicitly so valid + invalid + null = total.
- **Reconciliation proves it:** 173,912 valid / 0 quarantined on this run, but the framework is the point, not the count. The pattern surfaces bad data instead of hiding it.

## Gold dimensional model

- **Fact grain, stated first:** picked the grain before building `fact_assessment` = one row per student per assessment (173,912); `fact_vle` = aggregated engagement (8,459,320); `fact_registration` = one row per registration (32,593). Grain is the single most important modeling decision; everything else follows from it.
- **SCD2 evaluated, built, then dropped (the judgment story):** first built `dim_student` as SCD2, but the 72 students with attribute variation (28,857 distinct combos vs 28,785 students) created multiple "current" rows that **fanned out** the fact joins (173,912 to 174,726). Root cause: OULAD records attributes per-registration with **no temporal change timeline**, so SCD2 had nothing real to version. Rebuilt as one-row-per-student via a `row_number()` window (partition by id_student, order by code_presentation, keep rn = 1). Framing: *"SCD2 is the right design for a source with real temporal change; here it wasn't, and forcing it corrupted the grain. Judgment over rote pattern-matching."*
- **Composite-key joins or fan-out:** facts join dimensions on the **full** key (e.g. `code_module + code_presentation`), not a partial one, a partial join key fans out. Caught and verified via row-count reconciliation after each join.
- **Skinny facts:** facts carry surrogate keys + measures only; descriptive attributes live in dimensions. Keeps facts narrow and the star clean.
- **Verify, don't trust the preview:** `fact_registration` preview *looked* all-true on is_withdrawn, but a `groupBy` confirmed the real split (10,072 true / 22,521 false). A 10-row preview is not validation, aggregate to confirm.
- **Surrogate keys:** generated with `monotonically_increasing_id()`; natural keys are composite, so surrogates clean up the joins downstream.

## Semantic model & DAX

- **Direct Lake = the SSAS-cube replacement:** Power BI reads Delta straight from OneLake, import-like speed on live data, no scheduled refresh, no DirectQuery latency. The headline "why Fabric" answer most candidates can't give.
- **Conformed dimension:** `dim_student` feeds all three facts through single-direction many-to-one relationships, one dimension, consistent filtering across the star.
- **Assume referential integrity:** enabled on fact -> dim relationships *because* the zero-orphan check passed first, it speeds Direct Lake joins but is only safe once RI is validated. Enabling it without the check would be a latent bug.
- **DAX discipline:** `DIVIDE()` not `/` (null-safe on divide-by-zero); a "per X" measure names its denominator (caught an inverted clicks-per-student early); weighted score via `SUMX` + `RELATED` to reach across the relationship.

## BI validation & the Python follow-up

- **Group-size checks before trusting a headline finding:** the education gradient (23%–43% withdrawal) initially looked driven by two small extreme groups (n=252, n=306). Checked directly — the gradient holds across all five bands, including the two largest (12,355 and 11,780 students), so the small groups extend a real pattern rather than create one. Same discipline applied to region (all 11 groups >1,000 students — safe) and IMD band (all bands >2,200 — safe, and the cleanest gradient of the three).
- **A vague finding turned into a null result, deliberately:** "pass rates and scores vary by region" was checked and found false — withdrawal is flat (23%–35%) across all 11 UK regions, no meaningful pattern. Kept as a finding anyway, reframed as "geography isn't where this outcome gap lives" — ruling something out is as useful to a stakeholder as finding something, and more honest than manufacturing a pattern from noise.
- **Caught a real threshold bug via source documentation, not intuition:** the `pass rate` measure used `score > 40`; the original OULAD paper (Kuzilek, Hlosta, & Zdrahal, 2017) states 40 is the passing threshold, i.e. `>= 40`. Fixed the operator, re-ran, every per-module pass rate shifted up (89–98% → 91–98%) — students scoring exactly 40 had been silently misclassified as failing.
- **Distinguished "engagement varies by module" from "engagement predicts outcome":** the report visually shows a 20x spread in average clicks across modules — suggestive, not proof of a student-level relationship. Built a separate Python analysis (pandas/scipy, connected to Gold via Entra ID device-code auth) to test it properly: point-biserial correlation r = -0.356, p < 0.000001, n = 32,593 — students who stayed enrolled clicked a median of 994 times vs. 90 for students who withdrew, an 11x gap. Real and substantial, but documented explicitly as correlation, not causation — a causal claim would need to control for prior academic history and module.
- **BI and Python as complementary layers, not redundant ones:** the semantic model surfaces the question (engagement looks uneven); the correlation test answers it (does it actually predict outcome). Deliberately kept the Python scope narrow — one hypothesis, one test — rather than open-ended exploration once a stats environment was available.

## Governance dynamic RLS

- **Dynamic, not static (the production pattern):** RLS uses a security mapping table
  (`user_email to region`) plus `USERPRINCIPALNAME()`, so access is data-driven, add a user by inserting a row, no role edits. Static per-role rules don't scale; dynamic mapping is how it's done in production.
- **The rule:** on `dim_student`, `[region] IN CALCULATETABLE(VALUES(security_map[region]), security_map[user_email] = USERPRINCIPALNAME())`. The filter propagates from dim_student through the star to all three facts.
- **Mapping table kept unrelated to the star:** queried via DAX lookup rather than a relationship, to avoid a many-to-many that would mis-filter.
- **OLS evaluated, not implemented:** `imd_band` (socioeconomic indicator) identified as the column worth restricting, same layer as RLS (semantic model, role-based) would be the consistent choice over SQL-endpoint CLS, but Fabric's web-based semantic model editor doesn't expose OLS natively; it requires Tabular Editor over the XMLA endpoint. Framing for the interview: *"I identified the column, chose the right layer for consistency with my existing RLS, and the blocker was tooling access, not the decision, same kind of judgment call as the SCD2 evaluation, just not yet built."*

## Orchestration

- **Modern SSIS control flow:** a Fabric Data Pipeline chains Bronze ingest to Silver to Gold to semantic refresh, on-success, the direct equivalent of an SSIS control flow + SQL Agent schedule, scheduled weekly.
- **Bronze via Invoke Pipeline, not inline:** `pl_bronze_ingest` stays its own reusable, independently-testable pipeline; `pl_medallion_orchestration` invokes it as the first step rather than duplicating its 7 Copy activities. Separation of concerns over one giant pipeline.
- **Proved the notebooks are idempotent:** running the chain unattended end-to-end (Silver 3m18s, Gold 3m23s, refresh 41s, all succeeded) proved the notebooks run clean top-to-bottom, not "works if I run cells by hand in the right order." That's a real reproducibility signal.

## Performance: OPTIMIZE / V-Order

- **Small-files problem, demonstrated:** `OPTIMIZE` compacts `fact_vle` from several small files down to one (8.46M rows), file counts vary by run (8 files to 1, 9 files to 1, etc.), so the before/after is captured live in the notebook rather than cited as a fixed number. Fewer files = fewer open/close ops = faster Direct Lake reads. V-Order (on by default in Fabric) is preserved through compaction for read performance.
- **Honest scope:** correct demonstration of the technique on a small (~20MB) table; the dramatic wins are at production scale (billions of rows, thousands of files). Claim the *pattern*, size the *impact* honestly.

## The legacy monolith (the "before")

- **Built a real anti-pattern artifact:** one monolithic T-SQL `SELECT … INTO` that joins raw tables and does all cleaning/casting/aggregation inline, no layering, no reconciliation, no lineage, mixed grain.
- **Correlated subqueries (named, not hidden):** avg_score and total_clicks computed with correlated subqueries that re-scan per student, the row-by-row legacy pattern. The medallion replaces them with set-based aggregation that runs once.
- **DISTINCT is a band-aid, not a grain fix:** if a join fans out, the fix is the correct composite key, not a `DISTINCT` masking the duplicates. *"A DISTINCT hiding a fan-out means I don't understand my grain."*
- **CTEs aren't layers:** "no layering" is architecture (no staging, no materialized intermediate steps), not syntax, a CTE is still one monolithic pass.

## Updated quotable metrics

- 10,655,280 raw clickstream events to 8,459,320 aggregated (lossless, reconciled via `sum(event_count)`).
- 28,785 students · 32,593 registrations · 173,912 assessment submissions · 206 assessment definitions · 22 modules.
- ~31% withdrawal rate; education gradient 43% (no quals) to 23% (postgrad).
- 0 orphan keys across all fact to dim joins. OPTIMIZE compacts fact_vle from several small files to one.
- Pass rates 89–98% to attrition is withdrawal, not failure.
