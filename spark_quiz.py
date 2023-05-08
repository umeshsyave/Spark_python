# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

config= SparkConf().setAppName("Read")
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

rdd=sc.textFile('/FileStore/tables/sent.txt')

# COMMAND ----------

rdd.collect()

# COMMAND ----------



# COMMAND ----------

def func(x):
    li=[]
    for item in x.split():
        li.append(len(item))
    return li
rdd2=rdd.map(func)

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

rdd3= rdd.map(lambda x: [len(a) for a in x.split()])

# COMMAND ----------

rdd3.collect()

# COMMAND ----------

