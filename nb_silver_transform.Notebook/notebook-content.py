# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "1926c013-7667-418f-93e9-4b00b5c1c917",
# META       "default_lakehouse_name": "silver",
# META       "default_lakehouse_workspace_id": "e481a74a-9f2c-4567-9c87-43590f602fc7",
# META       "known_lakehouses": [
# META         {
# META           "id": "1926c013-7667-418f-93e9-4b00b5c1c917"
# META         },
# META         {
# META           "id": "877ea43f-8970-4584-b7e9-6fb0a71f28f4"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ## Silver Layer: Clean, Cast, Conform
# 
# Reads raw Bronze Delta tables and produces typed, cleaned, conformed Silver tables.
# Bronze preserves the source as-is (strings, nulls, duplicates and all); Silver is
# where every data-quality decision from `docs/diagnostics.sql` actually gets applied.
# 
# Three things happen in every table below, in order: **cast** (string > real type),
# **clean** (nulls, formatting, flags), **write** (overwrite mode full-reload
# idempotent, per the ingestion decisions in `docs/interview-notes.md`).

# CELL ********************

# Import libraries
from pyspark.sql import functions as F
from pyspark.sql.functions import col, when 
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### studentInfo
# 
# Casts `studied_credits` and `num_of_prev_attempts` to integer. Cleans `imd_band`:
# nulls/empty > `"Unknown"` (3.4% of rows low enough to impute, not drop), and
# standardizes the `"10-20"` > `"10-20%"` formatting gap found during diagnostics,
# so it doesn't silently split one category into two in any GROUP BY.

# CELL ********************

# starting with studentInfo table transformations  
df_student = spark.read.table("bronze.dbo.studentInfo")

df_student.printSchema()
df_student.show(5)

# Cast new datatypes from string to integers and timestamp 
df_student = df_student.withColumn("studied_credits", col("studied_credits").cast("integer"))
df_student = df_student.withColumn("num_of_prev_attempts", col("num_of_prev_attempts").cast("integer"))
df_student = df_student.withColumn("_ingested_at", col("_ingested_at").cast("timestamp"))

df_student.printSchema()

#  Case When imd_band col Null or empty equals 'Unkown'
df_student = df_student.withColumn("imd_band", when((col("imd_band") == "") | (col("imd_band").isNull()), "Unknown")
.otherwise(col("imd_band"))
)

# Case When imd_band 10-20 add % sign to maintain formatting concistency 
df_student = df_student.withColumn("imd_band", when((col("imd_band") == "10-20"), "10-20%")
.otherwise(col("imd_band"))
)

df_student.select("imd_band").distinct().show()

# write df to table in silver layer .dbo studentInfo Overwriting it (Perfect mode for static data)
df_student.write.format("delta").mode("overwrite").saveAsTable("silver.dbo.studentInfo")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### studentRegistration
# 
# Derives `is_withdrawn` from `date_unregistration` *before* casting it flagging
# off the raw string, not the cast value, so the flag logic doesn't depend on the
# cast succeeding first. ~31% of rows are withdrawals; kept and flagged, never
# dropped  this is the dataset's largest analytically-interesting cohort
# (retention analysis), and dropping it would erase that signal entirely.

# CELL ********************

# studentRegistration table
df_registration = spark.read.table("bronze.dbo.studentRegistration")

# Flag first, off the raw string column (empty/null = not withdrawn)
df_registration = df_registration.withColumn(
    "is_withdrawn",
    when((col("date_unregistration") == "") | (col("date_unregistration").isNull()), False).otherwise(True)
)

# cast: day-offsets are integers, not timestamps; _ingested_at is a real timestamp
df_registration = df_registration.withColumn("date_registration", col("date_registration").cast("integer"))
df_registration = df_registration.withColumn("date_unregistration", col("date_unregistration").cast("integer"))
df_registration = df_registration.withColumn("_ingested_at", col("_ingested_at").cast("timestamp"))

df_registration.printSchema()
df_registration.groupBy("is_withdrawn").count().show()
df_registration.write.format("delta").mode("overwrite").saveAsTable("silver.dbo.studentRegistration")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### assessments
# 
# Casts `date` (day-offset, not a real date) and `weight` to their proper types.
# `weight` specifically needed a distinct-values check first some values like
# `7.5` meant `integer` would silently truncate, so this is `double`.

# CELL ********************

# assesments table
df_assessments = spark.read.table("bronze.dbo.assessments")

# Cast: date to integers, weight to double and _ingested_at as timestamp 
df_assessments = df_assessments.withColumn("date", col("date").cast("integer"))
df_assessments = df_assessments.withColumn("weight", col("weight").cast("double"))
df_assessments = df_assessments.withColumn("_ingested_at", col("_ingested_at").cast("timestamp"))

df_assessments.printSchema()
df_assessments.show(5)

# write to silver as delta table on 'overwrite mode'
df_assessments.write.format("delta").mode("overwrite").saveAsTable("silver.dbo.assessments")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### courses
# 
# Smallest, cleanest table (22 rows) just a type cast on
# `module_presentation_length`. No data-quality issues surfaced here during
# diagnostics, which is itself worth knowing: not every table needs the same
# level of cleaning effort.

# CELL ********************

# courses table
df_courses = spark.read.table("bronze.dbo.courses")

df_courses = df_courses.withColumn("module_presentation_length", col("module_presentation_length").cast("integer"))
df_courses = df_courses.withColumn("_ingested_at", col("_ingested_at").cast("timestamp"))

df_courses.printSchema()
df_courses.show(5)

# write to silver as delta table on 'overwrite mode'
df_courses.write.format("delta").mode("overwrite").saveAsTable("silver.dbo.courses")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### vle
# 
# Site/material reference table (6,364 rows). Casts `week_from`/`week_to` to
# integer. Feeds `studentVle` downstream via `id_site` kept separate rather
# than pre-joined, since Gold is where dimensional joins belong, not Silver.

# CELL ********************

#vle table
df_vle = spark.read.table("bronze.dbo.vle")

# cast
df_vle = df_vle.withColumn("week_to", col("week_to").cast("integer"))
df_vle = df_vle.withColumn("week_from", col("week_from").cast("integer"))
df_vle = df_vle.withColumn("_ingested_at", col("_ingested_at").cast("timestamp"))

df_vle.printSchema()
df_vle.show(5)

# write to silver as delta table on 'overwrite mode'
df_vle.write.format("delta").mode("overwrite").saveAsTable("silver.dbo.vle")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### studentVle / clickstream aggregation, not dedup
# 
# This is the one table where `row_number()` dedup (used elsewhere) doesn't apply:
# clicks are additive, so deduping would silently lose real activity. Instead,
# `groupBy(id_student, code_module, code_presentation, id_site, date).agg(sum, count)`
# collapses repeat rows into one per student-material-day while preserving total
# clicks. `event_count` is the reconciliation column  `sum(event_count)` must equal
# the source row count (10,655,280) to prove the aggregation lost nothing.

# CELL ********************

df_studentVle = spark.read.table("bronze.dbo.studentVle")

df_studentVle = df_studentVle.withColumn("sum_click", col("sum_click").cast("integer"))
df_studentVle = df_studentVle.withColumn("date", col("date").cast("integer"))
df_studentVle = df_studentVle.withColumn("_ingested_at", col("_ingested_at").cast("timestamp"))
source_row_count = df_studentVle.count()

df_studentVle.printSchema()
df_studentVle.show(5)

df_student_vle_agg = df_studentVle.groupBy(
    "id_student", "code_module", "code_presentation", "id_site", "date"
).agg(
    F.sum("sum_click").alias("total_clicks"),
    F.count("*").alias("event_count")
)

df_student_vle_agg.write.format("delta").mode("overwrite").saveAsTable("silver.dbo.studentVle")

# Reconcile:
agg_event_total = df_student_vle_agg.agg(F.sum("event_count")).collect()[0][0]
assert agg_event_total == source_row_count, \
    f"studentVle aggregation lost rows: source={source_row_count}, event_count sum={agg_event_total}"
print(f"studentVle reconciliation: PASSED — {df_student_vle_agg.count()} aggregated rows, "
      f"{agg_event_total} events (matches source: {source_row_count}).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### studentAssessment quarantine framework
# 
# Splits into two outputs instead of one: valid rows (score 0–100, or null,
# a null score means "not yet graded," not corrupt) go to Silver; anything
# outside that range goes to a quarantine table instead of being silently
# dropped. The quarantine table is written even when empty (it is, on this
# dataset) that documents the check ran, not just that the data happened
# to be clean.

# CELL ********************

# studentAssessment table — cast + quarantine framework

df_studentAssessment = spark.read.table("bronze.dbo.studentAssessment")

# Cast: score & date_submitted to int, is_banked string->int->boolean, _ingested_at to timestamp
# (is_banked two-step cast: "0"/"1" string won't map directly to boolean, so int first)
df_studentAssessment = df_studentAssessment.withColumn("score", col("score").cast("integer"))
df_studentAssessment = df_studentAssessment.withColumn("date_submitted", col("date_submitted").cast("integer"))
df_studentAssessment = df_studentAssessment.withColumn("is_banked", col("is_banked").cast("integer").cast("boolean"))
df_studentAssessment = df_studentAssessment.withColumn("_ingested_at", col("_ingested_at").cast("timestamp"))
source_assessment_count = df_studentAssessment.count()

# QUARANTINE: split valid vs invalid score rows (validity rule: 0-100)
# Nulls -> valid (a null score = assessment not taken/graded, missing not corrupt)
# isNull() explicit in valid filter so nulls don't vanish from both filters
valid_studentAssessment = df_studentAssessment.filter(
    (col("score").isNull()) | ((col("score") >= 0) & (col("score") <= 100))
)
# Invalid = out-of-range only (corrupt data); 0 rows on this dataset (scores are clean)
invalid_studentAssessment = df_studentAssessment.filter(
    (col("score") < 0) | (col("score") > 100)
)

# Write valid -> silver, invalid -> quarantine table (kept even if empty: documents the check ran)
valid_studentAssessment.write.format("delta").mode("overwrite").saveAsTable("silver.dbo.studentAssessment")
invalid_studentAssessment.write.format("delta").mode("overwrite").saveAsTable("silver.dbo.studentAssessment_quarantine")

# Reconcile: valid + invalid must equal source 173,912 (no rows vanish)
valid_count = valid_studentAssessment.count()
invalid_count = invalid_studentAssessment.count()
assert valid_count + invalid_count == source_assessment_count, \
    f"studentAssessment quarantine split lost rows: source={source_assessment_count}, valid+invalid={valid_count + invalid_count}"
print(f"studentAssessment reconciliation: PASSED — {valid_count} valid, {invalid_count} quarantined "
      f"(matches source: {source_assessment_count}).")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

