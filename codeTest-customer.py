# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 15:10:33 2021

@author: 12016
"""

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SortedWordCount")
sc= SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    customerID = int(fields[0])
    dollarAmount = float(fields[2])
    return (customerID, dollarAmount)

line= sc.textFile("file:///SparkCourse/customer-orders.csv")

mappedInput= line.map(parseLine)
totalByCustomer = mappedInput.reduceByKey(lambda x, y: x + y)
results= totalByCustomer.collect()

for result in results:
    print(str(result[0]) +":\t" + str(result[1]) )
