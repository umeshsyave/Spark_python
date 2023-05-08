# Databricks notebook source
from pyspark import SparkConf,SparkContext

# COMMAND ----------

config=SparkConf().setAppName("Read")

# COMMAND ----------

sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

rdd=sc.textFile('/FileStore/tables/imp.txt')

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd2= rdd.map(lambda x: x.split(' '))

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

def func(x):
    l=[]
    for item in x:
        l.append(int(item)+1)
    return l

rdd3=rdd2.map(func)

# COMMAND ----------

rdd3.collect()

# COMMAND ----------

rdd4= rdd2.map(lambda x: [type(a) for a in x])
rdd4.collect()

# COMMAND ----------

rdd5= rdd2.map(lambda x: [int(a) for a in x])
rdd5.collect()

# COMMAND ----------

