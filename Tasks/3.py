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
    return avgRatings

# Subtask b) What are the top 10 most frequent categories in the data?
def mostFrequentCategories():
    textFile = sc.textFile(businessesPath)
    headers = textFile.first()
    textFileWithoutHeaders = textFile.filter(lambda line: line != headers)
    arrayOfCategories = textFileWithoutHeaders.map(lambda line: line.split("\t")[10:])
    listOfCategoriesUnix = arrayOfCategories.flatMap(lambda list: list)
    arrayOfCategoriesAscii = listOfCategoriesUnix.map(lambda category: str(category)).map(lambda category: category.split(","))
    listOfCategoriesAscii = arrayOfCategoriesAscii.flatMap(lambda list: list)
    categoryFrequencies = listOfCategoriesAscii.map(lambda category: (category, 1)).reduceByKey(lambda a,b: a+b)
    topTenCategories = categoryFrequencies.takeOrdered(10, key = lambda x: -x[1])
    lines = sc.parallelize(topTenCategories)
    lines.saveAsTextFile("output3b")
    return topTenCategories


# Subtask c)
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
    return centroids

    
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

    print("-------- (3b) Top 10 most frequent categories: --------")
    categories = mostFrequentCategories()
    for category in categories:
        print(category[0], category[1])

    print("-------- (3c) Geographical centroids for businesses for each postal code: --------")
    postalCodeCentroids()

if __name__ == "__main__":
    main()
