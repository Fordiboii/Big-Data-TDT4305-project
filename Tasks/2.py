import findspark
import base64
findspark.init("/home/olekfur/spark")

from pyspark import SparkContext, SparkConf

reviewerspath = "../data/yelp_top_reviewers_with_reviews.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

# 2 a) How many distinct users are there in the dataset
def distinctUsers(rdd):
    newRdd = rdd.map(lambda x: x.split()[1]).distinct()
    return newRdd.count()

# 2 b) How many what is the average number of the characters in a user review
def avgNumOfCharsInReview(rdd):
    newRdd = rdd.map(lambda x: x.split()[3]).filter(lambda y: y != u'"review_text"')
    numberOfReviews = newRdd.count()
    totalLength = newRdd.map(lambda review: len(base64.b64decode(review))).reduce(lambda a, b: a + b)
    return totalLength/numberOfReviews

if __name__ == "__main__":
    reviewersTextFile = sc.textFile(reviewerspath)

    #print("2 a) Distinct users: ")
    #print(distinctUsers(reviewersTextFile))
    print("2 b) Avg. no. of chars in review: ")
    print(avgNumOfCharsInReview(reviewersTextFile))

# 2 c) What is the business_id of the top 10 businesses with the most reviews
def top10BusinessesWithMostReviews():
    print("c): Top 10 businesses with the most reviews")
    textFile = sc.textFile(reviewerspath)
    reviewLinesRdd = textFile.map(lambda line: line.split('\t'))
    countPerBusinessRdd = reviewLinesRdd.map(lambda fields: (fields[2], 1)).reduceByKey(lambda count1, count2: count1 + count2)
    countPerBusinessTop10 = countPerBusinessRdd.takeOrdered(10, key = lambda x: -x[1])
    for business in countPerBusinessTop10:
        print(business[0])
        print(business[1])

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