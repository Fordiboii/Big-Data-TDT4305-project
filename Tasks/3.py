import findspark
import base64
findspark.init("/home/olekfur/spark")
import operator

from pyspark import SparkContext, SparkConf

businessesPath = "../data/yelp_businesses.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

# 3 a) What is the average rating for businesses in each city
def sumFunc(a, b):
    return a+b

def averageCityRating():
    print("e): Average rating for businesses in each city")
    textFile = sc.textFile(businessesPath)
    headers = textFile.first()
    textFile = textFile.filter(lambda line: line != headers)
    businessesLinesRdd = textFile.map(lambda line: line.split('\t'))
    citiesWithNumberOfReviews = businessesLinesRdd.map(lambda fields: (fields[3], fields[8])).countByKey()
    citiesWithAggregatedRatings = businessesLinesRdd.map(lambda fields: (fields[3], fields[8])).aggregateByKey(0,lambda v1,v2: v1+1\
,operator.add).collect()
    for x in range(10):
        print(citiesWithNumberOfReviews[x])


averageCityRating()