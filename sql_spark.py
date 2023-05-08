# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

spark= SparkSession.builder.appName('sql_spark').getOrCreate()

# COMMAND ----------

df=spark.read.csv('/FileStore/tables/googleplaystore.csv',inferSchema=True,header=True)

# COMMAND ----------

df.limit(5).toPandas()

# COMMAND ----------

df.describe()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

sizeformat= udf(lambda x: float(x.replace("M",'')),FloatType())


df2=df.withColumn('Size',sizeformat(df.Size))

# COMMAND ----------

df2.show()

# COMMAND ----------

installformat=udf(lambda x: x.replace('+',''),StringType())
installformat2= udf(lambda x: int(x.replace(',','')),IntegerType())

# COMMAND ----------

df3=df2.withColumn('Installs',installformat(df2.Installs))
df4=df3.withColumn('Installs',installformat2(df3.Installs))


# COMMAND ----------

df4.limit(5).toPandas()

# COMMAND ----------

df.columns

# COMMAND ----------

df.printSchema()

# COMMAND ----------

custom_schema=StructType([
    StructField('App',StringType(),True),
    StructField('Category',StringType(),True),
    StructField('Rating',FloatType(),True),
    StructField('Reviews',IntegerType(),True),
    StructField('Size',FloatType(),True),
    StructField('Installs',IntegerType(),True),
    StructField('Type',StringType(),True),
    StructField('Price',IntegerType(),True),
    StructField('Content Rating',StringType(),True),
    StructField('Genres',StringType(),True),
    StructField('Last Updated',DateType(),True),
    StructField('Current Ver',StringType(),True),
    StructField('Android Ver',StringType(),True)
    
])

# COMMAND ----------

data=spark.read.schema(custom_schema).csv('/FileStore/tables/googleplaystore.csv',header=True)

# COMMAND ----------

data.limit(5).toPandas()

# COMMAND ----------

data.columns

# COMMAND ----------

data.printSchema()

# COMMAND ----------

data.select(data.Size,data.Rating,data.Price,data.Installs).summary('min','max','count','stddev').toPandas()

# COMMAND ----------

data.createOrReplaceTempView('viewtable')

# COMMAND ----------

sql=spark.sql('select * from viewtable')

# COMMAND ----------

sql.show()

# COMMAND ----------

spark.sql('select Category,SUM(Reviews) AS No_Reviews from viewtable GROUP By Category ORDER BY No_Reviews desc').show()

# COMMAND ----------

max_review=spark.sql('select MAX(Reviews) AS High_price from viewtable')

# COMMAND ----------

print(max_review['High_price'])

# COMMAND ----------

spark.sql('select App,Category,Reviews,Rating from viewtable WHERE Reviews=781 ').show()

# COMMAND ----------

