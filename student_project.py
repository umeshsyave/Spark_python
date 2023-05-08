# Databricks notebook source
from pyspark import SparkConf, SparkContext

# COMMAND ----------

config=SparkConf().setAppName('project')
sc=SparkContext.getOrCreate(conf=config)

# COMMAND ----------

rdd=sc.textFile('/FileStore/tables/student_data.txt')
header=rdd.first()
data=rdd.filter(lambda x: x!=header)
data.collect()

# COMMAND ----------

# number of students in file
data.count()

# COMMAND ----------

# total marks acheived by female and male students
marks=data.map(lambda x: (x.split(',')[1],int(x.split(',')[5]))).reduceByKey(lambda x,y: x+y)
marks.collect()

# COMMAND ----------

# number of students passed and failed, 50+ marks is passing
pass_fail=data.map(lambda x: ('pass' if int(x.split(',')[5])>50 else 'fail',1)).reduceByKey(lambda x,y: x+y)
pass_fail.collect()

# COMMAND ----------

#number of students enrolled per course
students_per_course=data.map(lambda x: (x.split(',')[3],1)).reduceByKey(lambda x,y: x+y)
students_per_course.collect()

# COMMAND ----------

#total marks student achieved per course
marks_course=data.map(lambda x: (x.split(',')[3],int(x.split(',')[5]))).reduceByKey(lambda x,y: x+y)
marks_course.collect()

# COMMAND ----------

#average marks achived per course
avg_marks_course=data.map(lambda x: (x.split(',')[3],(int(x.split(',')[5]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1]))
avg_marks_course.collect()

# COMMAND ----------

#min  marks achived in course
min_marks=data.map(lambda x: (x.split(',')[3],int(x.split(',')[5]))).reduceByKey(lambda x,y: x if x<=y else y)
min_marks.collect()

# COMMAND ----------

# max marks achived in course
max_marks=data.map(lambda x: (x.split(',')[3],int(x.split(',')[5]))).reduceByKey(lambda x,y: x if x>=y else y)
max_marks.collect()

# COMMAND ----------

#avg age of male and female students
avg_age=data.map(lambda x: (x.split(',')[1],(int(x.split(',')[0]),1))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).map(lambda x: (x[0],x[1][0]/x[1][1]))
avg_age.collect()

# COMMAND ----------

