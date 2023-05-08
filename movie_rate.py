# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

config=SparkConf().setAppName('rated')
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

data= sc.textFile('/FileStore/tables/movie_file.txt')
data.collect()

# COMMAND ----------

data1= data.map(lambda x: ( x.split(',')[0],int(x.split(',')[1]) ))
data1.collect()

# COMMAND ----------

# max rating given  for minimumm rating given change the comparison operator
data1.reduceByKey(lambda x,y: x if x>=y else y).collect()

# COMMAND ----------

#average rating
data1.map(lambda x: (x[0],(x[1],1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1])).collect()

# COMMAND ----------

