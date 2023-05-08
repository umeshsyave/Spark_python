# Databricks notebook source
from pyspark import SparkConf,SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,sum,avg,min,max
from pyspark.sql.types import IntegerType,StringType,DoubleType

# COMMAND ----------

config=SparkConf().setAppName("rdds")
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

rdd=sc.textFile('/FileStore/tables/movies.csv')

# COMMAND ----------

rdd.collect()

# COMMAND ----------

fc=rdd.first()
rdd1=rdd.filter(lambda x: x!=fc)
rdd1.collect()

# COMMAND ----------



# COMMAND ----------

columns=fc.split(',')
rdd=rdd1.map(lambda x: x.split(','))
df=rdd1.toDF()


# COMMAND ----------

display(df)

# COMMAND ----------

