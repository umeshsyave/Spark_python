# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf,col,lit
from pyspark.sql.types import IntegerType,DoubleType

# COMMAND ----------

spark=SparkSession.builder.appName("user_defined_func").getOrCreate()
df=spark.read.options(header="True",inferschema="True").csv("/FileStore/tables/OfficeData.csv")

# COMMAND ----------

df.show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

udf_salary=udf(lambda x,y: x+y, IntegerType())

# COMMAND ----------

df=df.withColumn("total_salary",udf_salary(df.salary,df.bonus))

# COMMAND ----------

df.show()

# COMMAND ----------



# COMMAND ----------

def salary_increment(state,salary,bonus):
    sum=0
    if state=="NY":
        sum=0.1*salary
        sum+=0.05*bonus
        return sum
    elif state=="CA":
        sum=0.12*salary
        sum+=0.03*bonus
        return sum

increment_udf=udf(lambda x,y,z: salary_increment(x,y,z),DoubleType())

# COMMAND ----------

df=df.withColumn("increment",increment_udf(df.state,df.salary,df.bonus))

# COMMAND ----------

df.show()

# COMMAND ----------



# COMMAND ----------



# COMMAND ----------



# COMMAND ----------

