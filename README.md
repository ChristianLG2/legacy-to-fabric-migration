# Legacy-to-Fabric Migration

> End-to-end SQL Server → Microsoft Fabric migration implementing a medallion Lakehouse architecture, dimensional modeling, and a governed Direct Lake semantic layer.

This project re-architects a legacy SQL Server and SSIS analytics platform into a modern Microsoft Fabric ecosystem. Rather than performing a simple lift-and-shift migration, the objective is to rebuild the analytics stack using a versioned, governed, and scalable Lakehouse architecture capable of delivering near real-time business intelligence.

---

## Project Overview

Traditional analytics environments often suffer from tightly coupled ETL processes, limited scalability, and poor governance. This project demonstrates a complete modernization strategy by migrating a legacy SQL Server warehouse into Microsoft Fabric while maintaining data lineage, reconciliation capabilities, and enterprise governance standards.

The implementation follows the Medallion Architecture pattern (Bronze → Silver → Gold) and culminates in a Direct Lake semantic model optimized for Power BI reporting without scheduled refreshes.

---

## Problem Statement

The legacy environment consists of:

- A tightly coupled SQL Server data warehouse
- Monolithic SSIS packages
- No formal staging architecture
- Limited data lineage visibility
- Manual reprocessing workflows
- Minimal version control
- Performance bottlenecks on high-volume tables

The goal is to redesign the entire analytics platform into a scalable, maintainable, and governed Fabric solution while preserving a reconcilable path between the legacy and modern systems.

---

## Solution Architecture

```text
SQL Server + SSIS (Legacy)

          │
          │ Re-architect
          ▼

Bronze Layer
Raw Delta tables
Immutable • Replayable
(Data Pipeline ingestion)

          ▼

Silver Layer
Clean • Conform • Deduplicate
Data Quality Validation
Quarantine Handling
(PySpark transformations)

          ▼

Gold Layer
Kimball Star Schema
Dimensions • Facts
Surrogate Keys • SCD Type 2
(PySpark)

          ▼

Semantic Layer
Direct Lake Semantic Model
Power BI Reporting

Governance
Row-Level Security (RLS)
Column-Level Security (CLS)
Microsoft Entra ID

Orchestration
Fabric Data Pipelines

Version Control
Git-integrated Workspace
```

---

## Technology Stack

- Microsoft Fabric
- OneLake
- Delta Lake
- Fabric Data Pipelines
- PySpark
- Power BI (Direct Lake)
- DAX
- Microsoft Entra ID
- T-SQL
- Git

---

## Medallion Architecture

### Bronze Layer

Purpose: Preserve raw source data.

Responsibilities:

- Ingest data from SQL Server
- Store immutable Delta tables
- Enable replayability
- Preserve historical snapshots

---

### Silver Layer

Purpose: Standardize and improve data quality.

Responsibilities:

- Data cleansing
- Schema conformance
- Deduplication
- Data quality validation
- Quarantine invalid records

---

### Gold Layer

Purpose: Deliver analytics-ready datasets.

Responsibilities:

- Build Kimball dimensional models
- Generate surrogate keys
- Implement Slowly Changing Dimensions (SCD Type 2)
- Optimize for BI consumption

---

## Semantic Layer

The semantic layer is built using **Direct Lake**, allowing Power BI to query OneLake directly without traditional dataset refresh schedules.

Features include:

- Near real-time analytics
- Centralized business logic
- DAX measures
- Enterprise governance
- Reduced data duplication

---

## Governance

Security is implemented using Microsoft Entra ID.

Features:

- Row-Level Security (RLS)
- Column-Level Security (CLS)
- Centralized access management
- Workspace governance
- End-to-end data lineage

---

## Dataset

This project uses the **Open University Learning Analytics Dataset (OULAD)**, a real-world anonymized educational dataset containing approximately 32,000 students, assessments, and large-scale virtual learning environment clickstream data.

> **Note:** OULAD represents UK distance-learning data and is used to demonstrate the migration pattern on realistic, imperfect data. It does not represent a target institution.

Dataset: :contentReference[oaicite:0]{index=0}

---

## Project Goals

- Modernize a legacy SQL Server analytics platform
- Implement a Medallion Lakehouse architecture
- Establish data governance and security controls
- Build scalable dimensional models
- Eliminate refresh bottlenecks with Direct Lake
- Enable reproducible and version-controlled data engineering workflows

---

## Repository Structure

```text
├── docs/
│   ├── migration-strategy.md
│   ├── legacy-baseline.md
│   └── reconciliation-framework.md
│
├── pipelines/
│
├── notebooks/
│
├── semantic-model/
│
├── reports/
│
└── README.md
```

---

## Current Status

🚧 **Active Development**

Completed:

- Migration strategy definition
- Legacy environment assessment
- Target architecture design
- Reconciliation framework documentation

In Progress:

- Bronze ingestion pipelines
- Silver transformation layer
- Gold dimensional model
- Direct Lake semantic model
- Power BI dashboard development

Future Additions:

- Performance benchmarking
- End-to-end architecture walkthrough
- Data lineage visualization
- Monitoring and observability implementation

---

## Key Takeaway

This project demonstrates how to transform a traditional SQL Server + SSIS environment into a modern Microsoft Fabric platform using Lakehouse principles, dimensional modeling, and governed self-service analytics while preserving enterprise-grade reliability, traceability, and scalability.
