# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

config= SparkConf().setAppName('data_read')
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

items=sc.textFile('/FileStore/tables/grocery_items.txt')
items.collect()

# COMMAND ----------

items_list=items.flatMap(lambda x: x.split())
items_list.collect()

# COMMAND ----------

price=sc.textFile('/FileStore/tables/grocery_price.txt')
price.collect()

# COMMAND ----------

price_list=price.flatMap(lambda x: [int(a) for a in x.split()])
price_list.collect()

# COMMAND ----------

data=items_list.map(lambda x: (x,1))
data.collect()

# COMMAND ----------

data.groupByKey().collect()

# COMMAND ----------

data.reduceByKey(lambda x,y: (x+y)).collect()

# COMMAND ----------

