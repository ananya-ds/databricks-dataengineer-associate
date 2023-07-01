# Databricks notebook source
display(dbutils.fs.mounts())

# COMMAND ----------

display(dbutils.fs.ls("/mnt/adls/ADB Project/rawdata/countries/"))

# COMMAND ----------

countries_df.display()

# COMMAND ----------

countries_df_1 = spark.read.options(header = True).csv("/mnt/adls/ADB Project/rawdata/countries/countries.csv")

# COMMAND ----------

countries_df_1.display()

# COMMAND ----------

countries_df_1.dtypes

# COMMAND ----------

countries_df_1.printSchema()

# COMMAND ----------

countries_df_1.schema

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, IntegerType, StringType, DoubleType

# COMMAND ----------

 countres_schema = StructType([StructField('COUNTRY_ID', StringType(), True), 
                               StructField('NAME', StringType(), True), 
                               StructField('NATIONALITY', StringType(), True), 
                               StructField('COUNTRY_CODE', StringType(), True), 
                               StructField('ISO_ALPHA2', StringType(), True), 
                               StructField('CAPITAL', StringType(), True), 
                               StructField('POPULATION', DoubleType(), True), 
                               StructField('AREA_KM2', IntegerType(), True), 
                               StructField('REGION_ID', IntegerType(), True), 
                               StructField('SUB_REGION_ID', IntegerType(), True), 
                               StructField('INTERMEDIATE_REGION_ID', IntegerType(), True),
                               StructField('ORGANIZATION_REGION_ID', IntegerType(), True)])

# COMMAND ----------

countries_df = spark.read.csv("/mnt/adls/ADB Project/rawdata/countries/countries.csv", header=True, schema = countres_schema)

# COMMAND ----------

countries_df.dtypes

# COMMAND ----------

spark.read.options(header = True).schema(countres_schema).csv("/mnt/adls/ADB Project/rawdata/countries/countries.csv")

# COMMAND ----------

display(spark.read.options(multiline = True).json("dbfs:/mnt/adls/ADB Project/rawdata/countries/countries_multi_line.json"))

# COMMAND ----------


