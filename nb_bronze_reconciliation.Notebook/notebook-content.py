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
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!

from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("pl_diagnostic") \
    .getOrCreate()

tables = ["assessments", "courses", "studentAssessment", "studentInfo",
          "studentRegistration", "studentVle", "vle"]

for t in tables:
    count = spark.read.table(t).count()
    print(f"{t}: {count}")




# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
