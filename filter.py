# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

config= SparkConf().setAppName('filtering')
sc= SparkContext.getOrCreate(conf=config)
rdd= sc.textFile('/FileStore/tables/sent.txt')

# COMMAND ----------

rdd.collect()

# COMMAND ----------

rdd1= rdd.filter(lambda x: 'you' not in x)

# COMMAND ----------

rdd1.collect()

# COMMAND ----------



# COMMAND ----------

rdd2= rdd.filter(lambda x: 'you' in x)

# COMMAND ----------

rdd2.collect()

# COMMAND ----------

rdd3= rdd.flatMap(lambda x: x.split())
rdd3.collect()

# COMMAND ----------

rdd4= rdd3.filter(lambda x: x!='you')
rdd4.collect()

# COMMAND ----------

rdd5=rdd4.filter(lambda x: 'o' not in x)
rdd5.collect()

# COMMAND ----------

rdd6=rdd3.filter(lambda x: x[0]!='d' and x[0]!='a')
rdd6.collect()

# COMMAND ----------

num= sc.textFile('/FileStore/tables/imp.txt')

# COMMAND ----------

num.collect()

# COMMAND ----------

rdd.collect()

# COMMAND ----------

num1= num.map(lambda x: [int(a)%2 for a in x.split()])
num1.collect()

# COMMAND ----------

num2=num.flatMap(lambda x: x.split())
num2.collect()

# COMMAND ----------

num3= num2.filter(lambda x: int(x)%2!=0)
num3.collect()

# COMMAND ----------

