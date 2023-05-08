# Databricks notebook source
dbutils.fs.rm('/FileStore/tables/',True)

# COMMAND ----------

from pyspark.sql import SparkSession
spark=SparkSession.builder.appName('filtering').getOrCreate()

# COMMAND ----------

movdf=spark.read.csv('/FileStore/tables/movies.csv',inferSchema=True,header=True)
ratdf=spark.read.csv('/FileStore/tables/ratings.csv',inferSchema=True,header=True)

# COMMAND ----------

movdf.show(5)

# COMMAND ----------

ratdf.show(5)

# COMMAND ----------

movdf.printSchema()

# COMMAND ----------

ratdf.printSchema()

# COMMAND ----------

ratings=movdf.join(ratdf,'movieId','left')

# COMMAND ----------

ratings.show(5)

# COMMAND ----------

(train,test)=ratings.randomSplit([0.8,0.2])

# COMMAND ----------

train.count()

# COMMAND ----------

test.count()

# COMMAND ----------

from pyspark.ml.recommendation import ALS

als=ALS(userCol='userId',itemCol='movieId',ratingCol='rating',nonnegative=True,coldStartStrategy='drop',implicitPrefs=False)

# COMMAND ----------

from pyspark.ml.tuning import CrossValidator,ParamGridBuilder
from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

parm_grid=ParamGridBuilder().addGrid(als.rank,[10,50,100,150]).addGrid(als.regParam,[0.01,0.05,0.1,0.15]).build()

# COMMAND ----------

evaluator=RegressionEvaluator(metricName='rmse',labelCol='rating',predictionCol='prediction')

# COMMAND ----------

cv=CrossValidator(estimator=als,evaluator=evaluator,estimatorParamMaps=parm_grid,numFolds=5)

# COMMAND ----------

model=cv.fit(train)
bestmodel=model.bestModel
testpredict=bestmodel.transform(test)
rmse=evaluator.evaluate(testpredict)
print(rmse)

# COMMAND ----------

