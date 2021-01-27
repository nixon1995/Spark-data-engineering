# -*- coding: utf-8 -*-
"""
Created on Wed Jan 20 14:20:46 2021

@author: nixon abraham
"""

import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("SortedWordCount")
sc= SparkContext(conf = conf)

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())
input = sc.textFile("file:///sparkcourse/book.txt")
words= input.flatMap(normalizeWords)

wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()
results = wordCountsSorted.collect()


#run for loop convert int to str for printing 
# encode and decode words found to avoid Unicode error

for result in results:
    count = str(result[0])
    cleanWord= result[1].encode('ascii', 'ignore')
    
    if (cleanWord):
        print (cleanWord.decode()+ ":\t" + count)
