#importing spark librarys needed form pyspark package & import collection
# can’t create a sparkcontext without a sparkconf which allows you to configure the sparkcontext
from pyspark import SparkConf, SparkContext

# set the master to be local and assign a AppName : friendsByAge
conf = SparkConf().setMaster("local").setAppName("FriendsByAge")
sc = SparkContext(conf = conf)


#Column contents are srno, name, age , #friends
#parseLine function is developer to parse out important column data
#In this particular problem we would need Age and #friends, which are column 2,3 respectively
# to capture each element in row 2 and 3, it is necessary to split line with its delimiter (',')
# the return value for this function would be key value pair ( age, numfriends)
def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    numFriends = int(fields[3])
    return (age, numFriends)

#load data in initial RDD from external data set using method sc.textfile method
lines = sc.textFile("file:///SparkCourse/fakefriends.csv")

#second RDD contains key, value pair: age, numfriends
rdd = lines.map(parseLine)

#third RDD totalsByAge
totalsByAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

#mapValues(lambda x: (x, 1))-----> (33,385)-> (33,(385,1)) & (33,400)-> (33,(400,1))
#reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))----------> (33,(358+400, 1+1))


# fourth RDD averagesByAge
#mapvalue function to return key and average friends value
averagesByAge = totalsByAge.mapValues(lambda x: x[0] / x[1])

# collect all key value pair for avereges generated to results
results = averagesByAge.collect()
# print out all results element by element
for result in results:
    print(result)
