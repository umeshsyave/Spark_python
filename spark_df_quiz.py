# Databricks notebook source
from pyspark.sql import SparkSession

# COMMAND ----------

sc=SparkSession.builder.appName("data_frames").getOrCreate()

# COMMAND ----------

#data=sc.read.options(inferSchema="True",header="True").text("/FileStore/tables/student_data.txt")

# COMMAND ----------

from pyspark import SparkConf, SparkContext
config= SparkConf().setAppName("rdd_read")
sc=SparkContext.getOrCreate(conf=config)
rdd=sc.textFile("/FileStore/tables/student_data.txt")
header=rdd.first()
rdd= rdd.filter(lambda x: x!=header).map(lambda x: x.split(','))
rdd.collect()

# COMMAND ----------

data=rdd.toDF(header.split(','))
data.show()

# COMMAND ----------

data.printSchema()

# COMMAND ----------

from pyspark.sql.functions import col,lit

# COMMAND ----------

data=data.withColumn("age",col("age").cast('Int')).withColumn("marks",col("marks").cast('Int'))
data.printSchema()

# COMMAND ----------

data=data.withColumn("total_marks",lit(120)).withColumn("percentage",(col("marks")/col("total_marks"))*100)
data=data.withColumn("percentage",col("percentage").cast('Int'))
data.show()

# COMMAND ----------

data.printSchema()

# COMMAND ----------

grp1=data.filter((data.course=="OOP") & (data.percentage>=80))
grp1.show()

# COMMAND ----------

grp2=data.filter((data.course=="Cloud") & (data.percentage>=60))
grp2.show()

# COMMAND ----------

data.show()

# COMMAND ----------

data.select("name","marks").show()

# COMMAND ----------

