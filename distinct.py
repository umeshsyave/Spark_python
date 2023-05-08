# Databricks notebook source
from pyspark import SparkConf,SparkContext 

# COMMAND ----------

config=SparkConf().setAppName('distinct')
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

data=sc.textFile('/FileStore/tables/data_num.txt')
data.collect()

# COMMAND ----------

data.flatMap(lambda x: x.split()).collect()

# COMMAND ----------

data.flatMap(lambda x:x.split()).distinct().collect()

# COMMAND ----------

