# Legacy Baseline

_(In progress — full write-up Day 6.)_

Will cover, drawing on `docs/diagnostics.sql` and the migration:
- Legacy architecture: monolithic transform, no staging, no lineage, no version control
- Type debt: all columns loaded as strings (no enforcement)
- Data quality gaps: nulls (imd_band), inconsistent formats, duplicate clickstream events
- Why it's limiting → the case for re-architecting to medallion


