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

# CELL ********************

#
df_studentInfo = spark.read.table("silver.dbo.studentInfo")

# BUILD: the dimension DataFrame 
df_dim_student = df_studentInfo.select(
    "id_student","gender","region","highest_education",
    "imd_band","age_band","disability"
).distinct()

# MEASURE: numbers for analysis 
unique_students = df_studentInfo.select("id_student").distinct().count()
distinct_combos = df_dim_student.count()
print(f"distinct students: {unique_students}")          # 28,785
print(f"distinct attribute combos: {distinct_combos}")  # 28,857
print(f"students with changed attributes: {distinct_combos - unique_students}")  # 72


df_dim_student = df_dim_student.withColumn("student_sk", F.monotonically_increasing_id())


print(df_dim_student.count())# 28,857
print(df_dim_student.select("student_sk").distinct().count())# must also be 28,857
df_dim_student.select("student_sk", "id_student").show(5)


df_dim_student =df_dim_student.withColumn("effective_from", F.current_date())
df_dim_student = df_dim_student.withColumn("effective_to", F.lit("9999-12-31").cast("date"))
df_dim_student = df_dim_student.withColumn("is_current", F.lit(True).cast("boolean"))

df_dim_student = df_dim_student.select(
    "student_sk", "id_student", "gender", "region", "highest_education",
    "imd_band", "age_band", "disability",
    "effective_from", "effective_to", "is_current"
)

df_dim_student.show(5)
df_dim_student.printSchema()

df_dim_student.write.format("delta").mode("overwrite").saveAsTable("gold.dbo.dim_student")
print(spark.read.table("gold.dbo.dim_student").count())  # 28,857

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

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

# CELL ********************

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
