# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark.sql.functions import sum,min,max,count,avg,mean 

# COMMAND ----------

session=SparkSession.builder.appName("quiz_groupBy").getOrCreate()
df=session.read.options(header="True",inferSchema="True").csv("/FileStore/tables/StudentData.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df=df.withColumn("roll",col("roll").cast("String"))
df.printSchema()

# COMMAND ----------

df.groupBy("course").agg(count("*").alias("enrollments")).show()

# COMMAND ----------

df.groupBy("course","gender").agg(count("*").alias("enrolments")).show()

# COMMAND ----------

df.groupBy("course","gender").agg(sum("marks").alias("total_marks")).show()

# COMMAND ----------

df.groupBy("course","age").agg(min("marks").alias("min_marks"),max("marks").alias("max_marks"),avg("marks").alias("avg_marks")).show()

# COMMAND ----------

