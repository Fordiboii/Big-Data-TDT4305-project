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
    textFile = sc.textFile(businessesPath)
    headers = textFile.first()
    textFile = textFile.filter(lambda line: line != headers)
    businessesLinesRdd = textFile.map(lambda line: line.split('\t'))
    citiesAndRatings = businessesLinesRdd.map(lambda fields: (fields[3], float(fields[8])))
    avgRatings = citiesAndRatings.aggregateByKey((0, 0), lambda  a,b:(a[0] + b, a[1]+ 1), lambda a,b: (a[0] + b[0], a[1] + b[1])).mapValues(lambda v: str(round(v[0]/v[1], 1)))
    lines = avgRatings.map(toCSVLine)
    lines.saveAsTextFile("output3a")
    print(avgRatings.collect())


def postalCodeCentroids():
    textFile = sc.textFile(businessesPath)
    headers = textFile.first()
    textFile = textFile.filter(lambda line: line != headers)
    businessLinesRdd = textFile.map(lambda line: line.split('\t'))
    codeLat = postalCodeLat(businessLinesRdd)
    codeLong = postalCodeLong(businessLinesRdd)
    avgLat = averageCoordinate(codeLat)
    avgLong = averageCoordinate(codeLong)
    centroids = avgLat.join(avgLong)
    lines = centroids.map(tuplesToCSVLine)
    lines.saveAsTextFile("output3c")

    print(centroids.collect())

    
def postalCodeLat(rdd):
    return rdd.map(lambda fields: (fields[5], float(fields[6])))

def postalCodeLong(rdd):
    return rdd.map(lambda fields: (fields[5], float(fields[7])))

def averageCoordinate(postalCoordList):
    return postalCoordList.aggregateByKey((0,0), lambda a,b:(a[0] + b, a[1] + 1), lambda a,b: (a[0] + b[0], a[1] + b[1])).mapValues(lambda v: round(v[0]/v[1], 3))

def toCSVLine(data):
    return '\t'.join(d.encode('utf-8') for d in data)

def tuplesToCSVLine(data):
    return '\t' .join(str(d) for d in data)


def main():
    print("-------- (3a) Average business-ratings for each city: --------")
    averageCityRating()

    print("-------- (3c) Geographical centroids for businesses for each postal code: --------")
    postalCodeCentroids()

if __name__ == "__main__":
    main()
