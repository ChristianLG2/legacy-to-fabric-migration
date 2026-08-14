## Quarantine & validation framework

- **Quarantine over drop (the maturity signal):** invalid `studentAssessment`
  scores are routed to a quarantine table, not silently dropped valid rows
  (score 0–100) flow to Silver, out-of-range rows are isolated for review. Drop
  loses evidence; quarantine preserves it and makes the DQ rule auditable.
- **The null-in-both-filters trap:** a naive "valid" filter (`score BETWEEN 0 AND
  100`) and "invalid" filter (`score < 0 OR score > 100`) *both* silently exclude
  NULLs so nulls vanish from both outputs and the counts don't reconcile to the
  source. Fix: handle NULL explicitly so valid + invalid + null = total.
- **Reconciliation proves it:** 173,912 valid / 0 quarantined on this run but the
  framework is the point, not the count. The pattern surfaces bad data instead of
  hiding it.

## Gold dimensional model

- **Fact grain, stated first:** picked the grain before building `fact_assessment`
  = one row per student per assessment (173,912); `fact_vle` = aggregated engagement
  (8,459,320); `fact_registration` = one row per registration (32,593). Grain is the
  single most important modeling decision; everything else follows from it.
- **SCD2 evaluated, built, then dropped (the judgment story):** first built
  `dim_student` as SCD2 but the 72 students with attribute variation (28,857
  distinct combos vs 28,785 students) created multiple "current" rows that **fanned
  out** the fact joins (173,912 → 174,726). Root cause: OULAD records attributes
  per-registration with **no temporal change timeline**, so SCD2 had nothing real to
  version. Rebuilt as one-row-per-student via a `row_number()` window (partition by
  id_student, order by code_presentation, keep rn = 1). Framing: *"SCD2 is the right
  design for a source with real temporal change; here it wasn't, and forcing it
  corrupted the grain. Judgment over rote pattern-matching."*
- **Composite-key joins or fan-out:** facts join dimensions on the **full** key
  (e.g. `code_module + code_presentation`), not a partial one a partial join key
  fans out. Caught and verified via row-count reconciliation after each join.
- **Skinny facts:** facts carry surrogate keys + measures only; descriptive
  attributes live in dimensions. Keeps facts narrow and the star clean.
- **Verify, don't trust the preview:** `fact_registration` preview *looked* all-true
  on is_withdrawn, but a `groupBy` confirmed the real split (10,072 true / 22,521
  false). A 10-row preview is not validation aggregate to confirm.
- **Surrogate keys:** generated with `monotonically_increasing_id()`; natural keys
  are composite, so surrogates clean up the joins downstream.

## Semantic model & DAX

- **Direct Lake = the SSAS-cube replacement:** Power BI reads Delta straight from
  OneLake import-like speed on live data, no scheduled refresh, no DirectQuery
  latency. The headline "why Fabric" answer most candidates can't give.
- **Conformed dimension:** `dim_student` feeds all three facts through single-direction
  many-to-one relationships one dimension, consistent filtering across the star.
- **Assume referential integrity:** enabled on fact→dim relationships *because* the
  zero-orphan check passed first it speeds Direct Lake joins but is only safe once
  RI is validated. Enabling it without the check would be a latent bug.
- **DAX discipline:** `DIVIDE()` not `/` (null-safe on divide-by-zero); a "per X"
  measure names its denominator (caught an inverted clicks-per-student early);
  weighted score via `SUMX` + `RELATED` to reach across the relationship.

## Governance dynamic RLS

- **Dynamic, not static (the production pattern):** RLS uses a security mapping table
  (`user_email → region`) plus `USERPRINCIPALNAME()`, so access is data-driven add a
  user by inserting a row, no role edits. Static per-role rules don't scale; dynamic
  mapping is how it's done in production.
- **The rule:** on `dim_student`,
  `[region] IN CALCULATETABLE(VALUES(security_map[region]), security_map[user_email] =
  USERPRINCIPALNAME())`. The filter propagates from dim_student through the star to all
  three facts.
- **Mapping table kept unrelated to the star:** queried via DAX lookup rather than a
  relationship, to avoid a many-to-many that would mis-filter.

## Orchestration

- **Modern SSIS control flow:** a Fabric Data Pipeline chains Silver → Gold → semantic
  refresh, on-success the direct equivalent of an SSIS control flow + SQL Agent
  schedule, scheduled daily.
- **Proved the notebooks are idempotent:** running the chain unattended end-to-end
  (Silver 3m18s, Gold 3m23s, refresh 41s, all succeeded) proved the notebooks run
  clean top-to-bottom not "works if I run cells by hand in the right order." That's
  a real reproducibility signal.

## Performance OPTIMIZE / V-Order

- **Small-files problem, demonstrated:** `OPTIMIZE` compacted `fact_vle` from several small files to one. ** (8.46M rows). Fewer files = fewer open/close ops = faster Direct Lake
  reads. V-Order (on by default in Fabric) is preserved through compaction for read performance.
- **Honest scope:** correct demonstration of the technique on a small (~20MB) table;
  the dramatic wins are at production scale (billions of rows, thousands of files).
  Claim the *pattern*, size the *impact* honestly.

## The legacy monolith (the "before")

- **Built a real anti-pattern artifact:** one monolithic T-SQL `SELECT … INTO` that
  joins raw tables and does all cleaning/casting/aggregation inline no layering, no
  reconciliation, no lineage, mixed grain.
- **Correlated subqueries (named, not hidden):** avg_score and total_clicks computed
  with correlated subqueries that re-scan per student the row-by-row legacy pattern.
  The medallion replaces them with set-based aggregation that runs once.
- **DISTINCT is a band-aid, not a grain fix:** if a join fans out, the fix is the
  correct composite key, not a `DISTINCT` masking the duplicates. *"A DISTINCT hiding a
  fan-out means I don't understand my grain."*
- **CTEs aren't layers:** "no layering" is architecture (no staging, no materialized
  intermediate steps), not syntax\ a CTE is still one monolithic pass.

## Updated quotable metrics

- 10,655,280 raw clickstream events → 8,459,320 aggregated (lossless, reconciled via
  `sum(event_count)`).
- 28,785 students · 32,593 registrations · 173,912 assessments · 206 assessments · 22 modules.
- ~31% withdrawal rate; education gradient 43% (no quals) → 23% (postgrad).
- 0 orphan keys across all fact→dim joins. OPTIMIZE compacts fact_vle from several small files to one.
- Pass rates 89–98% → attrition is withdrawal, not failure.
