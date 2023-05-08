# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

spark= SparkSession.builder.appName('dataframe').getOrCreate()

# COMMAND ----------

data=spark.read.options(inferSchema='True', header='True').csv("/FileStore/tables/student_data.txt")

# COMMAND ----------

data.show()

# COMMAND ----------

data.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField,IntegerType,StringType

# COMMAND ----------

schema=StructType([
    StructField("age",IntegerType(),True),
    StructField("gender",StringType(),True),
    StructField("name",StringType(),True),
    StructField("course",StringType(),True),
    StructField("roll",StringType(),True),
    StructField("marks",IntegerType(),True),
    StructField("email",StringType(),True),
])

# COMMAND ----------

table= spark.read.options(header='True').schema(schema).csv('/FileStore/tables/student_data.txt')

# COMMAND ----------

table.show()

# COMMAND ----------

table.printSchema()

# COMMAND ----------

