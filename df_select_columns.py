# Databricks notebook source
from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('column_filter').getOrCreate()

# COMMAND ----------

data=spark.read.options(header='True',inferSchema='True').csv('/FileStore/tables/student_data.txt')

# COMMAND ----------

data.show()
data.printSchema()

# COMMAND ----------

data.select('age','name','roll').show()

# COMMAND ----------

data.select(data.name,data.course,data.marks).show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

data.select(col('name'),col('gender'),col('email')).show()

# COMMAND ----------

data.columns

# COMMAND ----------

data.select(data.columns[0:3]).show()

# COMMAND ----------

data.select(data.columns[3:]).show()

# COMMAND ----------

data.select(col('name'),data.age,'gender').show()

# COMMAND ----------

