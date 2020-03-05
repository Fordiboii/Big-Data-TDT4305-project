import findspark
import base64
findspark.init("/home/olekfur/spark")
from operator import add

from pyspark import SparkContext, SparkConf

businessesPath = "../data/yelp_businesses.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

# 3c) For each postal code in the busiess table, calculate the geographical
#centroid of the region shown by the postal code.

def postalCodeCentroids():
    textFile = sc.textFile(businessesPath)
    headers = textFile.first()
    textFile = textFile.filter(lambda line: line != headers)
    businessLinesRdd = textFile.map(lambda line: line.split('\t'))
    codeLat = postalCodeLat(businessLinesRdd)
    codeLong = postalCodeLong(businessLinesRdd)
    avgLat = averageCoordinate(codeLat)
    avgLong = averageCoordinate(codeLong)
    centroids = avgLat.join(avgLong).collect()
    return centroids

    
def postalCodeLat(rdd):
    return rdd.map(lambda fields: (fields[5], float(fields[6])))

def postalCodeLong(rdd):
    return rdd.map(lambda fields: (fields[5], float(fields[7])))

def averageCoordinate(postalCoordList):
    return postalCoordList.aggregateByKey((0,0), lambda a,b:(a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1])).mapValues(lambda v: round(v[0]/v[1], 3))

postalCodeCentroids()