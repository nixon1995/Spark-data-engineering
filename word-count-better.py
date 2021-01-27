
#import Regular Expresion to handle word processing 
import re
#import SparkCOnf and SparkContext libreary from pyspark package
from pyspark import SparkConf, SparkContext

#define a function that returns a lower case normalized words
#Normalizing we transform the all words into lower case so that we dont get different count for same words based on case sensitivity
def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

#Set up sparkcontext object 
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

# read data from source loaction
input = sc.textFile("file:///sparkcourse/book.txt")

#utilizing flat map to capture various words after calling normalizeWord
words = input.flatMap(normalizeWords)
wordCounts = words.countByValue()

for word, count in wordCounts.items():
    cleanWord = word.encode('ascii', 'ignore')
    if (cleanWord):
        print(cleanWord.decode() + " " + str(count))
