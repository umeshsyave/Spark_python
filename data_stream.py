# Databricks notebook source
from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

# COMMAND ----------

config=SparkConf().setAppName("data_stream")
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

ssc=StreamingContext(sc,5)

# COMMAND ----------

rdd=ssc.textFileStream('/FileStore/tables')

# COMMAND ----------

rdd.pprint()


ssc.start()
ssc.awaitTerminationOrTimeout(100)

# COMMAND ----------



# COMMAND ----------

