# Databricks notebook source
from pyspark import  SparkConf, SparkContext

# COMMAND ----------

config= SparkConf().setAppName('grouping')
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

rdd=sc.textFile('/FileStore/tables/sent.txt')
rdd.collect()

# COMMAND ----------

rdd1=rdd.flatMap(lambda x: x.split())
rdd1.collect()

# COMMAND ----------

rdd2= rdd1.map(lambda x: (x,len(x)))
rdd2.collect()

# COMMAND ----------

rdd3=rdd2.map(lambda x: x[::-1])
rdd3.collect()

# COMMAND ----------

rdd3.groupByKey().collect()  #creates iterable object

# COMMAND ----------

rdd3.groupByKey().mapValues(list).collect()   #getting the iterable object and mapping

# COMMAND ----------

rdd3.groupByKey().mapValues(tuple).collect()

# COMMAND ----------

