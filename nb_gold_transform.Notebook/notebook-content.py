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
# META           "id": "95e8d8bb-b489-43ff-b5a7-37323ff27ac5"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql.functions import col, when 
from pyspark.sql.window import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Dimension tables
# 
# #### 1. `dim_student_info` table:

# MARKDOWN ********************

# First attempt used SCD2 (effective dates + is_current). Changed to one-row-per-student because: (a) OULAD has no temporal change-tracking, so versioning isn't meaningful, and (b) the 72 students with attribute variation created multiple "current" rows, which fanned out the fact join (173,912 → 174,726). Documented SCD2 as the design I'd apply given a source with real temporal change data.


# CELL ********************

# # Student info dimension table

# # read silver studentInfo table
# df_studentInfo = spark.read.table("silver.dbo.studentInfo")

# # BUILD: the dimension DataFrame 
# df_dim_student = df_studentInfo.select(
#     "id_student","gender","region","highest_education",
#     "imd_band","age_band","disability"
# ).distinct()

# # MEASURE: numbers for analysis 
# unique_students = df_studentInfo.select("id_student").distinct().count()
# distinct_combos = df_dim_student.count()
# print(f"distinct students: {unique_students}")          # 28,785
# print(f"distinct attribute combos: {distinct_combos}")  # 28,857
# print(f"students with changed attributes: {distinct_combos - unique_students}")  # 72


# df_dim_student = df_dim_student.withColumn("student_sk", F.monotonically_increasing_id())


# print(df_dim_student.count())# 28,857
# print(df_dim_student.select("student_sk").distinct().count())# must also be 28,857
# df_dim_student.select("student_sk", "id_student").show(5)


# df_dim_student =df_dim_student.withColumn("effective_from", F.current_date())
# df_dim_student = df_dim_student.withColumn("effective_to", F.lit("9999-12-31").cast("date"))
# df_dim_student = df_dim_student.withColumn("is_current", F.lit(True).cast("boolean"))

# df_dim_student = df_dim_student.select(
#     "student_sk", "id_student", "gender", "region", "highest_education",
#     "imd_band", "age_band", "disability",
#     "effective_from", "effective_to", "is_current"
# )

# df_dim_student.show(5)
# df_dim_student.printSchema()

# df_dim_student.write.format("delta").mode("overwrite").saveAsTable("gold.dbo.dim_student")
# print(spark.read.table("gold.dbo.dim_student").count())  # 28,857

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# dim_student: evaluated SCD2 (effective dates + is_current) but switched to one-row-per-student. OULAD records attributes per-registration with no temporal change timeline, so versioning isn't meaningful and the 72 students with attribute variation created multiple rows that fanned out the fact join (173,912 → 174,726). Used row_number() over a window (partition by id_student) to deterministically keep one row per student. SCD2 documented as the design for a source with real temporal change data.

# CELL ********************

df_studentInfo = spark.read.table("silver.dbo.studentInfo")

w = Window.partitionBy("id_student").orderBy("code_presentation")

df_dim_student = (
    df_studentInfo
    .withColumn("rn", F.row_number().over(w))
    .filter(col("rn") == 1)
    .select("id_student", "gender", "region", "highest_education", "imd_band", "age_band", "disability")
)

# Surrogate key
df_dim_student = df_dim_student.withColumn("student_sk", F.monotonically_increasing_id())

# Order SK-first
df_dim_student = df_dim_student.select(
    "student_sk","id_student","gender","region",
    "highest_education","imd_band","age_band","disability"
)

# Verify + write
print(df_dim_student.count())                                  # expect 28,785 now
print(df_dim_student.select("id_student").distinct().count())  # must also be 28,785 (unique!)
df_dim_student.write.format("delta").mode("overwrite").saveAsTable("gold.dbo.dim_student")
print(spark.read.table("gold.dbo.dim_student").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 2. `dim_courses` table:


# CELL ********************

# Courses dimension table

# read silver courses table
df_dim_module = spark.read.table("silver.dbo.courses")

df_dim_module = df_dim_module.select("code_module", "code_presentation", "module_presentation_length").distinct()
df_dim_module = df_dim_module.withColumn("module_sk",F.monotonically_increasing_id())

df_dim_module = df_dim_module.select("module_sk", "code_module", "code_presentation", "module_presentation_length")

df_dim_module.write.format("delta").mode("overwrite").saveAsTable("gold.dbo.dim_module")
print(spark.read.table("gold.dbo.dim_module").count())  # expect 22

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 3.`dim_assessments` table: 

# CELL ********************

# assessments dimension table

# read silver assessments table
df_assessment_dim = spark.read.table("silver.dbo.assessments")

df_assessment_dim = df_assessment_dim.select("code_module", "code_presentation", "id_assessment", "assessment_type", "date", "weight").distinct()
df_assessment_dim = df_assessment_dim.withColumn("assessment_sk", F.monotonically_increasing_id())

df_assessment_dim = df_assessment_dim.select("assessment_sk", "code_module", "code_presentation", "id_assessment", "assessment_type", "date", "weight")

df_assessment_dim.write.format("delta").mode("overwrite").saveAsTable("gold.dbo.dim_assessment")
print(spark.read.table("gold.dbo.dim_assessment").count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Fact tables
# #### 1.`fact_assessment` table
# :

# CELL ********************

dim_student = spark.read.table("gold.dbo.dim_student")
dim_assessment = spark.read.table("gold.dbo.dim_assessment")
df_studentAssessment = spark.read.table("silver.dbo.studentAssessment")

fact_assessment = df_studentAssessment \
    .join(dim_student.select("student_sk", "id_student"), on="id_student", how="left") \
    .join(dim_assessment.select("assessment_sk", "id_assessment"), on="id_assessment", how="left")

fact_assessment.printSchema()
print(fact_assessment.count())  

fact_assessment = fact_assessment.select(
    "student_sk", "assessment_sk", "score", "is_banked", "date_submitted"
)
fact_assessment.write.format("delta").mode("overwrite").saveAsTable("gold.dbo.fact_assessment")
print(spark.read.table("gold.dbo.fact_assessment").count())  # 173,912

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 

# MARKDOWN ********************

# #### 2.`dim_module` table:

# CELL ********************

dim_module = spark.read.table("gold.dbo.dim_module")
dim_student = spark.read.table("gold.dbo.dim_student")
df_student_vle = spark.read.table("silver.dbo.studentVle")

fact_student_vle = df_student_vle \
    .join(dim_student.select("student_sk", "id_student"), on="id_student", how="left") \
    .join(
        dim_module.select("module_sk", "code_module", "code_presentation"),
        on=["code_module", "code_presentation"],   # BOTH columns composite key
        how="left"
    )

# null-key checks (left-join responsibility)
print(fact_student_vle.filter(col("student_sk").isNull()).count())  # expect 0
print(fact_student_vle.filter(col("module_sk").isNull()).count())   # expect 0
print(fact_student_vle.count())  # expect 8,459,320 — confirm NO fan-out

# skinny fact: keys + measurements
fact_student_vle = fact_student_vle.select(
    "student_sk", "module_sk", "total_clicks", "event_count"
)
fact_student_vle.write.format("delta").mode("overwrite").saveAsTable("gold.dbo.fact_vle")
print(spark.read.table("gold.dbo.fact_vle").count())  # 8,459,320

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
