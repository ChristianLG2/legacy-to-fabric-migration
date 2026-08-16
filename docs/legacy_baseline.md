# Legacy System Assessment — The "Before" State

This document characterizes the legacy reporting workload that this project replaces. It outlines the original architecture, its accumulated technical debt, and the reasons a complete re-architecture was preferable to a simple lift-and-shift migration.
The analysis is based on `docs/diagnostics.sql` and the monolithic transformation implemented in `legacy/monolithic_transform.sql`.


## Architecture

The legacy workload consists of a single SQL warehouse (`legacy_dw`) containing seven raw OULAD tables. These tables are transformed by **one monolithic T-SQL script** that joins all sources into a wide, denormalized reporting table in a single execution.
Several characteristics of this design create significant maintainability and reliability challenges:

- **No architectural layering.** Data flows directly from raw ingestion to the final reporting output in a single statement. There are no staging layers, intermediate materializations, or checkpoints for validation and debugging.
- **No data lineage.** The system does not track when data was processed, how transformations were applied, or where values originated.
- **No version control.** Transformation logic exists as ad-hoc SQL rather than reviewable, reproducible, and versioned code.
- **No idempotency guarantees.** The process drops and recreates its output without validating that repeated executions produce identical results.
- **Inefficient row-by-row processing patterns.** Aggregations rely on correlated subqueries that repeatedly scan the 10.7-million-row clickstream dataset for each student instead of using a single set-based aggregation strategy.


## Type Debt

Every column in `legacy_dw` is stored as a **string**, with no type enforcement.

As a result, the system cannot guarantee that a score is numeric, a date represents a date, or a flag behaves as a boolean. This introduces several risks:
- Numeric fields (`score`, `sum_click`, `weight`) must be cast every time they are used. An incorrect cast can silently corrupt data. For example, casting `weight` values such as `7.5` to an integer would truncate the decimal portion.
- Fields with date-like names (`date_registration`, `date_unregistration`, `date`) are not timestamps but **day-offset integers**. The naming convention does not reflect the underlying representation, forcing downstream consumers to inspect values before trusting them.


## Data Quality Gaps

The source data contains real, measurable quality issues that must be explicitly addressed.
- **Missing values.** Approximately 3.4% of students have a null `imd_band`. If left untreated, analyses grouped by this field will silently exclude those records.
- **Inconsistent formats.** `imd_band` values appear in multiple representations (e.g., `10-20` versus `10-20%`), causing logically identical categories to be treated as separate values.
- **Duplicate clickstream events.** `studentVle` contains multiple records for the same student-material-day combination. The raw dataset contains 10.7 million rows, but only approximately 8.5 million unique events after aggregation to a meaningful grain.
- **Withdrawals.** Approximately 31% of registrations end in withdrawal. This is analytically significant but can be unintentionally omitted if not explicitly modeled.
- **Referential integrity validation.** Cross-table validation found no orphan records between `studentInfo` and `studentRegistration`; however, this was only discoverable through an explicit anti-join, as the legacy pipeline performed no integrity checks.


## Why Re-Architect Instead of Lift-and-Shift?

The legacy workload is functional, but it is not maintainable, observable, or extensible.

There is no way to inspect intermediate results, trace data lineage, reproduce historical transformations, or systematically validate outputs. In addition, inefficient processing patterns repeatedly scan the largest dataset, while underlying data quality issues remain hidden until they surface in downstream reports.
These are **architectural problems rather than platform limitations**. Simply migrating the existing implementation to newer infrastructure would preserve the same weaknesses.
Instead, the solution is a deliberate re-architecture into a **layered, typed, and reconciled medallion Lakehouse architecture**, where data quality checks, lineage, validation, and governance are built into the system by design.
The migration approach is described in `docs/migration-strategy.md`.