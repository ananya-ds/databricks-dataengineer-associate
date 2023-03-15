# Databricks notebook source
print(1+2)

# COMMAND ----------

df  = spark.range(1,100)

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.withColumn("sqrt", round(sqrt(col("id")),2)).show()

# COMMAND ----------

# MAGIC %sql
# MAGIC select "I am a Data Engineer"

# COMMAND ----------


