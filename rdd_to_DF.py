# Databricks notebook source
from pyspark import SparkConf, SparkContext
config=SparkConf().setAppName('rdd_read')
sc=SparkContext.getOrCreate(conf=config)
rdd=sc.textFile('/FileStore/tables/student_data.txt')

# COMMAND ----------

header=rdd.first()
rdd1=rdd.filter(lambda x: x!=header)
rdd1.collect()

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('data_frame').getOrCreate()

# COMMAND ----------

rdd2=rdd1.map(lambda x: x.split(','))
columns=header.split(',')
dataframe= rdd2.toDF(columns)
dataframe.show()
dataframe.printSchema()

# COMMAND ----------

from pyspark.sql.types import StructType,StructField,IntegerType,StringType

# COMMAND ----------

schema= StructType([
    StructField('age',IntegerType(),True),
    StructField('gender',StringType(),True),
    StructField('name',StringType(),True),
    StructField('course',StringType(),True),
    StructField('roll',StringType(),True),
    StructField('marks',IntegerType(),True),
    StructField('email',StringType(),True),
])

# COMMAND ----------

df_rdd= rdd2.map(lambda x: [int(x[0]),x[1],x[2],x[3],x[4],int(x[5]),x[6]])
data_frame= spark.createDataFrame(data=df_rdd,schema=schema)
data_frame.show()
data_frame.printSchema()

# COMMAND ----------

