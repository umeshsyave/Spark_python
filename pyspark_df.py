# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

sc=SparkSession.builder.appName('spark_read').getOrCreate()

# COMMAND ----------

df= sc.read.options(header="True",inferSchema="True").csv('/FileStore/tables/students-1.csv')

# COMMAND ----------

df.display()

# COMMAND ----------

df.limit(5).toPandas()

# COMMAND ----------

# DBTITLE 1,Validating
df.printSchema

# COMMAND ----------

df.describe()

# COMMAND ----------

df.columns

# COMMAND ----------

df.select('math score','reading score','writing score').summary('count','min','max').show()

# COMMAND ----------

