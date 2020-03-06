import findspark
import base64
from datetime import datetime
findspark.init("/home/olekfur/spark")

from pyspark import SparkContext, SparkConf

reviewerspath = "../data/yelp_top_reviewers_with_reviews.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

# 2 a) How many distinct users are there in the dataset
def distinctUsers():
    textFile = sc.textFile(reviewerspath)
    newRdd = textFile.map(lambda x: x.split()[1]).distinct()
    amount = newRdd.count()
    print(amount)
    return amount

# 2 b) How many what is the average number of the characters in a user review
def avgNumOfCharsInReview():
    textFile = sc.textFile(reviewerspath)
    newRdd = textFile.map(lambda x: x.split()[3]).filter(lambda y: y != u'"review_text"')
    numberOfReviews = newRdd.count()
    totalLength = newRdd.map(lambda review: len(base64.b64decode(review))).reduce(lambda a, b: a + b)
    print(totalLength/numberOfReviews)
    return totalLength/numberOfReviews

# 2 c) What is the business_id of the top 10 businesses with the most reviews
def top10BusinessesWithMostReviews():
    textFile = sc.textFile(reviewerspath)
    reviewLinesRdd = textFile.map(lambda line: line.split('\t'))
    countPerBusinessRdd = reviewLinesRdd.map(lambda fields: (fields[2], 1)).reduceByKey(lambda count1, count2: count1 + count2)
    countPerBusinessTop10 = countPerBusinessRdd.takeOrdered(10, key = lambda x: -x[1])
    sc.parallelize(countPerBusinessTop10).saveAsTextFile("output2c")
    for business in countPerBusinessTop10:
        print(business[0])
        print(business[1])
    return countPerBusinessTop10

# 2 d) Find the number of reviews per year
def timestampToYear(timestamp):
    return str(datetime.utcfromtimestamp(float(timestamp)))[0:4]

def numberofReviewsPerYear():
    textFile = sc.textFile(reviewerspath)
    unixTimestampsWithHead = textFile.map(lambda line: line.split()[4])
    head = unixTimestampsWithHead.first()
    unixTimestamps = unixTimestampsWithHead.filter(lambda timestamp: timestamp != head)
    reviewsPerYear = unixTimestamps.map(lambda stamp: (timestampToYear(stamp), 1)).reduceByKey(lambda a, b: a+b).collect()
    print(reviewsPerYear)
    sc.parallelize(reviewsPerYear).saveAsTextFile("output2d")
    return reviewsPerYear

# 2 e) What is the time and date of the first and last review

def timeAndDateOfFirstAndLastReview():
    print("e): Time and date of first and last review")
    textFile = sc.textFile(reviewerspath)
    headers = textFile.first()
    textFile = textFile.filter(lambda line: line != headers)
    reviewLinesRdd = textFile.map(lambda line: line.split('\t'))
    reviewDatesRdd = reviewLinesRdd.map(lambda fields: (fields[4]))
    minTime = reviewDatesRdd.reduce(lambda time1, time2: time1 if time1<time2 else time2)
    maxTime = reviewDatesRdd.reduce(lambda time1, time2: time1 if time1>time2 else time2)
    print("MinTime: " + str(minTime))
    print("MaxTime: " + str(maxTime))

def main():
    print("-------- (2a) How many distinct users are there in the dataset?")
    #distinctUsers()

    print("-------- (2b) What is the average number of characters in a user review? --------")
    #avgNumOfCharsInReview()

    print("-------- (2c) What is the business id of the top 10 businesses with the most reviews? --------")
    #top10BusinessesWithMostReviews()

    print("-------- (2d) What is the number of reviews per year? --------")
    numberofReviewsPerYear()

    print("-------- (2e) What is the time and date of the first and last review? --------")
    #timeAndDateOfFirstAndLastReview()


if __name__ == "__main__":
    main()