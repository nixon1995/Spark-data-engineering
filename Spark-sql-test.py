# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 15:53:13 2021

@author: nixon abraham
"""

#SparkSession and Row for using Sql session and dataFrame
#if you create a spark session also need to stop the sparksession at the end 
from pyspark.sql import SparkSession
from pyspark.sql import Row

#create spark session
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


#define mapper function
#dataFrames are just data sets of Row objects
#return value would be ROw object with each fields assigned with a column info
def mapper(line):
    fields= line.split(',')
    return Row(ID= int(fields[0]), name= str(fields[1].encode("utf-8")), \
               age= int(fields[2]), numFriends= int(fields[3]))
#now we have a Row object containing these explicity typed and named fields that are going to correspond to columns in our dataFrame


#we would need SparkContext: thats the API for using RDDs
lines= spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper)

#infer the schema and register the DataFrame as a table
schemaPeople= spark.createDataFrame(people).cache()
#cache the people dataFrame to run some SQL querries, we want to keep that in memory
#to querry that dataframe as a database table, created a view to use it like a database Table
schemaPeople.createOrReplaceTempView("people")

#SQL can run over DataFrames that have been registered as a table
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <=19")

#The result of SQL queries are RDD and support all the normal RDD Operations.
for teen in teenagers.collect():
    print(teen)


#Utilizing functions instead of SQL queries
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()
