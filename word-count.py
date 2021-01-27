
import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizedWord(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


line = sc.textFile("file:///sparkcourse/book.txt")
words = line.flatMap(normalizedWord)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
