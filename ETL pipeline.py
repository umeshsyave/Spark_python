# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit,col,explode
import pyspark.sql.functions as f
spark=SparkSession.builder.appName('etl_pipeline').getOrCreate()

# COMMAND ----------

df=spark.read.text('/FileStore/tables/etl_data.txt')

# COMMAND ----------

display(df)

# COMMAND ----------

df1=df.withColumn('splitdata',f.split('value',' ')).withColumn('words',explode('splitdata')).select('words')

# COMMAND ----------

display(df1)

# COMMAND ----------

ldf=df1.groupBy('words').count()
display(ldf)

# COMMAND ----------

