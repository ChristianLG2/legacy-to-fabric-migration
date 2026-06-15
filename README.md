# Legacy-to-Fabric Migration

> End-to-end SQL Server → Microsoft Fabric migration: a medallion Lakehouse,
> a dimensional model, and a governed Direct Lake semantic layer.

This project re-architects a legacy SQL Server / SSIS analytics platform into a
modern Microsoft Fabric ecosystem. Rather than a lift-and-shift, the objective is
to rebuild the analytics stack as a versioned, governed, scalable Lakehouse that
delivers near real-time business intelligence without scheduled refresh overhead.

## Problem

The legacy environment is a tightly coupled SQL Server warehouse loaded by a
monolithic SSIS job:

- No formal staging architecture
- Limited data lineage
- Manual reprocessing workflows
- Minimal version control
- Performance bottlenecks on high-volume tables

The goal is to redesign the platform into a scalable, maintainable, governed
Fabric solution while preserving a clear, **reconcilable** path between the legacy
and modern systems.

## Architecture

```text
SQL Server / SSIS (legacy)
        │  re-architect (not lift-and-shift)
        ▼
   Bronze    raw Delta · immutable · replayable        ← Copy activity (DB → lake)
        ▼
   Silver    clean · conform · dedupe · DQ + quarantine  ← PySpark
        ▼
   Gold      Kimball star: dims + facts · surrogate keys · SCD2  ← PySpark
        ▼
   Semantic model (Direct Lake) + Power BI report
```

- **Governance** — row- and column-level security via Microsoft Entra ID
- **Orchestration** — Fabric Data Pipeline schedule (replacing SQL Agent)
- **Versioning** — Git-integrated workspace

## Tech stack

Microsoft Fabric · OneLake · Delta Lake · Fabric Data Pipelines · PySpark ·
Power BI (Direct Lake) · DAX · Microsoft Entra ID (RLS/CLS) · T-SQL · Git

## Medallion layers

| Layer | Purpose | Key responsibilities |
| --- | --- | --- |
| **Bronze** | Preserve raw source data | Ingest from the legacy warehouse, store immutable Delta, enable replay, keep historical snapshots |
| **Silver** | Standardize & raise quality | Cleanse, conform schema, deduplicate, validate, quarantine invalid records |
| **Gold** | Deliver analytics-ready data | Kimball star schema, surrogate keys, SCD Type 2, BI-optimized model |

The **semantic layer** is built on **Direct Lake**, letting Power BI query OneLake
directly with no dataset refresh schedule — near real-time analytics, centralized
DAX business logic, and reduced data duplication. It is the modern equivalent of
the legacy SSAS cube.

## Dataset

Built on the [Open University Learning Analytics Dataset (OULAD)](https://analyse.kmi.open.ac.uk/open-dataset)
— real, anonymized educational data covering ~32,000 students, assessments, and a
large virtual-learning-environment clickstream.

> **Note:** OULAD is UK distance-learning data, used to demonstrate the migration
> *pattern* on realistic, imperfect data. It does not represent a target
> institution's data.

## Repository structure

```text
├── docs/
│   ├── migration-strategy.md       # as-is, to-be, mapping, approach, validation
│   ├── legacy-baseline.md          # the legacy system and why it's limiting
│   └── reconciliation-framework.md # legacy ↔ Fabric cutover validation
│
├── legacy/                         # the monolithic "old SSIS way" T-SQL transform
├── notebooks/                      # PySpark: Bronze → Silver → Gold
├── pipelines/                      # Fabric Data Pipeline definitions
├── semantic-model/                 # Direct Lake model + DAX measures
├── reports/                        # Power BI report
└── README.md
```

> Fabric Git integration syncs workspace **item definitions** (notebooks,
> pipelines, semantic models) into the repo automatically. Data stays in OneLake;
> only code and definitions are versioned.

## Status

🚧 **Active development**

Done:

- Fabric workspace provisioned (trial capacity)
- Bronze Lakehouse created
- Legacy SQL warehouse (`legacy_dw`) stood up and loaded with all seven OULAD
  source tables via a Copy job
- Load verified with T-SQL against the warehouse

In progress / next:

- Legacy baseline write-up and the monolithic transform
- Bronze ingestion (legacy warehouse → Lakehouse)
- Silver transformation layer (PySpark)
- Gold dimensional model
- Direct Lake semantic model and Power BI report
- Governance (RLS/CLS) and performance optimization
- Reconciliation framework and end-to-end walkthrough

## Key takeaway

A complete modernization of a traditional SQL Server + SSIS environment into
Microsoft Fabric using Lakehouse principles, dimensional modeling, and governed
self-service analytics — while preserving lineage, reconcilability, and
enterprise-grade reliability.
