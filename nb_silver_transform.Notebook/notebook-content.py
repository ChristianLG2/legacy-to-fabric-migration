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

# Welcome to your new notebook
# Type here in the cell editor to add code!

df_student = spark.read.table("bronze.dbo.studentInfo")

df_student.printSchema()
df_student.show(5)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col

df_student = df_student.withColumn("studied_credits", col("studied_credits").cast("integer"))
df_student = df_student.withColumn("num_of_prev_attempts", col("num_of_prev_attempts").cast("integer"))
df_student = df_student.withColumn("_ingested_at", col("_ingested_at").cast("timestamp"))

df_student.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import when

df_student = df_student.withColumn("imd_band", when((col("imd_band") == "") | (col("imd_band").isNull()), "Unknown")
.otherwise(col("imd_band"))
)

df_student = df_student.withColumn("imd_band", when((col("imd_band") == "10-20"), "10-20%")
.otherwise(col("imd_band"))
)

df_student.select("imd_band").distinct().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_student.write.format("delta").mode("overwrite").saveAsTable("silver.dbo.studentInfo")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
