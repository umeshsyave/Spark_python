# Databricks notebook source
from pyspark import SparkConf,SparkContext

# COMMAND ----------

config= SparkConf().setAppName('rating')
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

data= sc.textFile('/FileStore/tables/imdb_top250.csv')

# COMMAND ----------

data.collect()

# COMMAND ----------

data1=data.filter(lambda x: x!='movie,year,rating').map(lambda x: (  x.split(',')[1],x.split(',')[2]  )).filter(lambda x: len(x[1])==3).map(lambda x: (x[0],float(x[1])))
data1.collect()

# COMMAND ----------

data2= data1.map(lambda x: (x[0], (x[1],1)))
data2.collect()

# COMMAND ----------

data3= data2.reduceByKey(lambda x , y : ((x[0]+y[0]),(x[1]+y[1])) ).map(lambda x: (x[0], x[1][0]/x[1][1]) )
data3.collect()

# COMMAND ----------

