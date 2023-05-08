# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

config=SparkConf().setAppName("Read_file")

# COMMAND ----------

sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

text=sc.textFile("/FileStore/tables/imp.txt")

# COMMAND ----------

text.collect()

# COMMAND ----------

