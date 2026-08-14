# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "877ea43f-8970-4584-b7e9-6fb0a71f28f4",
# META       "default_lakehouse_name": "bronze",
# META       "default_lakehouse_workspace_id": "e481a74a-9f2c-4567-9c87-43590f602fc7",
# META       "known_lakehouses": [
# META         {
# META           "id": "877ea43f-8970-4584-b7e9-6fb0a71f28f4"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "3b54aabd-01fc-a4e8-41ea-039f69ba65f8",
# META       "known_warehouses": [
# META         {
# META           "id": "3b54aabd-01fc-a4e8-41ea-039f69ba65f8",
# META           "type": "Datawarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Bronze Layer: Ingestion Reconciliation
# 
# Bronze ingestion itself (`pl_bronze_ingest`) is a Fabric Data Pipeline  7 Copy
# activities, one per source table, `legacy_dw` > `bronze.Lakehouse`, each stamping
# an `_ingested_at` audit column via `@utcnow()`. Overwrite mode, so reruns replace
# cleanly rather than duplicating (the idempotency decision documented in
# `docs/interview-notes.md`).
# 
# This notebook is the verification step, not the ingestion itself: it reads both
# sides  source (`legacy_dw`) and destination (`bronze`)  for all 7 tables, and
# **asserts** row-count equality rather than just printing counts for someone to
# eyeball. A silent count mismatch here would mean the pipeline's Copy activities
# lost or duplicated rows somewhere, which is exactly the kind of failure that
# should stop the run loudly, not slip downstream into Silver unnoticed.

# CELL ********************

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("pl_diagnostic") \
    .getOrCreate()

tables = ["assessments", "courses", "studentAssessment", "studentInfo",
          "studentRegistration", "studentVle", "vle"]


print(f"{'table':<22}{'legacy_dw':>12}{'bronze':>12}{'match':>8}")
all_match = True
for t in tables:
    source_count = spark.read.table(f"legacy_dw.dbo.{t}").count()
    bronze_count = spark.read.table(f"bronze.dbo.{t}").count()
    match = source_count == bronze_count
    all_match = all_match and match
    print(f"{t:<22}{source_count:>12}{bronze_count:>12}{str(match):>8}")

assert all_match, "Bronze reconciliation failed — row counts do not match legacy_dw"
print("\nBronze reconciliation: PASSED — all 7 tables match source counts.")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
