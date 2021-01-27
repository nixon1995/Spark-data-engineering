# -*- coding: utf-8 -*-
"""
Created on Tue Jan 19 18:44:07 2021

@author: nixon
"""

from pyspark import SparkConf, SparkContext


conf = SparkConf().setMaster("local").setAppName("MaxTemp")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    stationId = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) +32.0
    return (stationId, entryType, temperature)

line = sc.textFile("file:///SparkCourse/1800.csv")
#parseLines consist of all the return values from the function
parseLines= line.map(parseLine)

#filter out all the TMAX 
maxTemp = parseLines.filter(lambda x : 'TMAX' in x[1])

#Store all values excluding entry type column make a key value pair
stationTemp = maxTemp.map(lambda x : (x[0], x[2]))

# perform some action to gain max temp as result
maximumTemp = stationTemp.reduceByKey(lambda x,y : max(x,y))

#results, use collection function 
results = maximumTemp.collect();


# iterate and print out the results
for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))