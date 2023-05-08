# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,lit,udf,sum,count,max,min,avg
from pyspark.sql.types import DoubleType,IntegerType

# COMMAND ----------

init=SparkSession.builder.appName("mini_project").getOrCreate()
df=init.read.options(header="True",inferschema="True").csv("/FileStore/tables/OfficeDataProject.csv")

# COMMAND ----------

df.show()


# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.select("employee_id").distinct().count()

# COMMAND ----------

df.select("department").distinct().count()

# COMMAND ----------

df.select("department").distinct().show()

# COMMAND ----------

df.groupBy("department").agg(count("employee_id").alias("No_of_employees")).show()

# COMMAND ----------

df.groupBy("state").agg(count("employee_id").alias("No_of_employees")).show()

# COMMAND ----------

df.groupBy("state","department").agg(count("employee_id").alias("No_of_employees")).orderBy("state").show()

# COMMAND ----------

df.groupBy("department").agg(min("salary").alias("minimum_salary"),max("salary").alias("maximum_salary")).orderBy("minimum_salary").show()

# COMMAND ----------



# COMMAND ----------

df1=df.filter((df.state=="NY") & (df.department=="Finance"))
df1.show()

# COMMAND ----------

avg_bonus=df1.groupBy("department").avg("bonus").collect()[0][1]

# COMMAND ----------

df1.filter(df1.bonus>avg_bonus).show()

# COMMAND ----------

raised_salary=udf(lambda x,y:x+500 if(y>45) else x, IntegerType())
df.withColumn("raised_salary",raised_salary(df.salary,df.age)).show()

# COMMAND ----------

df.show()

# COMMAND ----------

df2=df.filter(df.age>45)
df2.show()

# COMMAND ----------

df2.write.mode("overwrite").options(header="True").csv("/FileStore/tables/df_project_output")

# COMMAND ----------

