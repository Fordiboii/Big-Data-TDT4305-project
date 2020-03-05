import findspark
import base64
findspark.init("/home/olekfur/spark")
from operator import add

from pyspark import SparkContext, SparkConf

businessesPath = "../data/yelp_businesses.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

# 3 a) What is the average rating for businesses in each city

def averageCityRating():
    print("e): Average rating for businesses in each city")
    textFile = sc.textFile(businessesPath)
    headers = textFile.first()
    textFile = textFile.filter(lambda line: line != headers)
    businessesLinesRdd = textFile.map(lambda line: line.split('\t'))
    citiesAndRatings = businessesLinesRdd.map(lambda fields: (fields[3], float(fields[8])))

    #Helper-code

    #print("City and rating")
    #for city in citiesAndRatings:
    #    print(city)
    #print("///////////////////")
    #print("Number of reviews in each city")
    #citiesNumberOfReviews = sc.parallelize(citiesAndRatings).countByKey()
    #for city in citiesNumberOfReviews:
    #    print(str(city) + ": " + str(citiesNumberOfReviews[city]))
    #print("///////////////////")
    #print("Sum of reviews for each city")
    #citiesAggregatedRatings = sc.parallelize(citiesAndRatings).reduceByKey(lambda a,b: int(a)+int(b)).collect()
    #print(citiesAggregatedRatings)
    #print("////////////////////")
    #print("City with number of reviews and sum of reviews")
    #citiesNumberAndAggregate = sc.parallelize(citiesNumberOfReviews).union(sc.parallelize(citiesAggregatedRatings)).collect()
    #print(citiesNumberAndAggregate)
    #print("////////////////////")

    #

    avgRatings = citiesAndRatings.aggregateByKey((0, 0), lambda  a,b:(a[0] + b, a[1]+ 1), lambda a,b: (a[0] + b[0], a[1] + b[1])).mapValues(lambda v: round(v[0]/v[1], 1)).collect()
    print(avgRatings)
    return avgRatings


averageCityRating()