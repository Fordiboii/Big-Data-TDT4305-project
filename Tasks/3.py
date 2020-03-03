import findspark
import base64
findspark.init("/home/olekfur/spark")
from operator import add

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
    citiesAndRatings = businessesLinesRdd.map(lambda fields: (str(fields[3]), int(fields[8]))).take(20)
    print("City and rating")
    for city in citiesAndRatings:
        print(city)
    print("///////////////////")
    print("Number of reviews in each city")
    # countByKey returns a hashmap. TODO hashMap => key-value pair for later join with other rdd.
    citiesNumberOfReviews = sc.parallelize(citiesAndRatings).countByKey()
    for city in citiesNumberOfReviews:
        print(str(city) + ": " + str(citiesNumberOfReviews[city]))
    print("///////////////////")
    print("Sum of reviews for each city")
    citiesAggregatedRatings = sc.parallelize(citiesAndRatings).reduceByKey(lambda a,b: int(a)+int(b)).collect()
    print(citiesAggregatedRatings)
    print("////////////////////")
    print("City with number of reviews and sum of reviews")
    citiesNumberAndAggregate = sc.parallelize(citiesNumberOfReviews).union(sc.parallelize(citiesAggregatedRatings)).collect()
    print(citiesNumberAndAggregate)


averageCityRating()