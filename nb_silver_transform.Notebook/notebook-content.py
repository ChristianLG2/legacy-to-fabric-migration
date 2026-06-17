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

# CELL ********************

df_studentVle = spark.read.table("bronze.dbo.studentVle")

df_studentVle = df_studentVle.withColumn("sum_click", col("sum_click").cast("integer"))
df_studentVle = df_studentVle.withColumn("date", col("date").cast("integer"))
df_studentVle = df_studentVle.withColumn("_ingested_at", col("_ingested_at").cast("timestamp"))

df_studentVle.printSchema()
df_studentVle.show(5)

df_student_vle_agg = df_studentVle.groupBy(
    "id_student", "code_module", "code_presentation", "id_site", "date"
).agg(
    F.sum("sum_click").alias("total_clicks"),
    F.count("*").alias("event_count")
)

df_student_vle_agg.write.format("delta").mode("overwrite").saveAsTable("silver.dbo.studentVle")

# Reconcile two ways:
print(df_student_vle_agg.count())                              # new (smaller) row count
print(df_student_vle_agg.agg(F.sum("event_count")).show())     # must equal 10,655,280

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# studentAssessment table — cast + quarantine framework

df_studentAssessment = spark.read.table("bronze.dbo.studentAssessment")

# Cast: score & date_submitted to int, is_banked string->int->boolean, _ingested_at to timestamp
# (is_banked two-step cast: "0"/"1" string won't map directly to boolean, so int first)
df_studentAssessment = df_studentAssessment.withColumn("score", col("score").cast("integer"))
df_studentAssessment = df_studentAssessment.withColumn("date_submitted", col("date_submitted").cast("integer"))
df_studentAssessment = df_studentAssessment.withColumn("is_banked", col("is_banked").cast("integer").cast("boolean"))
df_studentAssessment = df_studentAssessment.withColumn("_ingested_at", col("_ingested_at").cast("timestamp"))

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
print(valid_studentAssessment.count())     # 173,912
print(invalid_studentAssessment.count())   # 0

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

