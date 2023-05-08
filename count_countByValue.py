# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

config=SparkConf().setAppName('data_process')
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

data=sc.textFile('/FileStore/tables/grocery_items.txt')

# COMMAND ----------

data.collect()

# COMMAND ----------

data.flatMap(lambda x: x.split()).collect()

# COMMAND ----------

data.flatMap(lambda x: x.split()).count()

# COMMAND ----------

data.flatMap(lambda x: x.split()).countByValue()

# COMMAND ----------

