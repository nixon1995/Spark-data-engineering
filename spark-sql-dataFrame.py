# -*- coding: utf-8 -*-
"""
Created on Fri Jan 22 17:03:03 2021

@author: nixon abraham
"""
#SparkSession for using Sql session and dataFrame
#if you create a spark session also need to stop the sparksession at the end 

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

people = spark.read.option("header","true").option("inferSchema", "true")\
    .csv("fakefriends-header.csv")
    
#print detailed about the schema
print("Infered schema")
people.printSchema()

#view the dataFrame columns
print("Name column")
people.select("name").show()

#filter function to return based on condition
print("filter out anyone over 21")
people.filter(people.age <21).show()

#GroupBy
print("Group By age")
people.groupBy("age").count().show()

#sorting the results
print("Check out this result")
people.groupBy("age").avg("friends").sort("age").show(30)

#imposing round function from function library fucntions as func
#then sort out by age
#then show 30 results
people.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends-avg")).sort("age").show(30)

print("make a new column with age increased by 10")
people.select(people.name, new =people.age +10).show()

spark.stop()