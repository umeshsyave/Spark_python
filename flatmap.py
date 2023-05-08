# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

config= SparkConf().setAppName("data_process")

# COMMAND ----------

sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

rdd= sc.textFile('/FileStore/tables/imp.txt')

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd1= rdd.map(lambda x: x.split())
rdd1.collect()

# COMMAND ----------

rdd2=rdd.flatMap(lambda x: x.split())
rdd2.collect()

# COMMAND ----------

rdd3=rdd.flatMap(lambda x: [2*int(a) for a in x.split()])
rdd3.collect()

# COMMAND ----------

