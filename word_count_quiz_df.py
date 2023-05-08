# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit
from pyspark.sql.functions import sum,avg,min,max,count

# COMMAND ----------

session= SparkSession.builder.appName("word_quiz").getOrCreate()

# COMMAND ----------

df=session.read.text("/FileStore/tables/WordData.txt")

# COMMAND ----------

df.show()

# COMMAND ----------

df1=df.withColumn("num",lit(1))
df1.show()

# COMMAND ----------

df1.groupBy("value").agg(sum("num").alias("count")).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.groupBy("value").count().show()

# COMMAND ----------

