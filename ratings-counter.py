
#importing spark librarys needed form pyspark package & import collection
from pyspark import SparkConf, SparkContext
import collections

# seting up configurations that we need using sparkConfig and seting App name to be RatingHIStogram
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf = conf)

# loading the u.datafile from dataset directory
# set master where you run a job, for now it's just local.
#initial RDD
lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")

#parse those data into different fields
#read text line by line, in each line it will split based on white space and select values at position 2
# returing value to a new RDD
ratings = lines.map(lambda x: x.split()[2])

#storing data in result and calling countByValue function that will actually split us the data for us.
#performing an action on RDD rating
result = ratings.countByValue()

#rating present only as Natural number 1,2,3,4,5 not 1,5 or 2,4
# outcome:  sort the result and print them out
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
