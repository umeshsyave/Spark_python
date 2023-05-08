# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

config=SparkConf().setAppName('saving')
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

rdd=sc.textFile('/FileStore/tables/grocery_items.txt')

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd.getNumPartitions()

# COMMAND ----------

rdd1=rdd.flatMap(lambda x: x.split()).map(lambda x: (x,1))

# COMMAND ----------

rdd1.collect()

# COMMAND ----------

rdd1.saveAsTextFile('/FileStore/tables/output1')

# COMMAND ----------

data=sc.textFile('/FileStore/tables/grocery_items.txt')

# COMMAND ----------

data_1=data.flatMap(lambda x: x.split())

# COMMAND ----------

data_1.collect()

# COMMAND ----------

data_1.getNumPartitions()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

