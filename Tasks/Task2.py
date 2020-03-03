import findspark
import base64
from datetime import datetime

findspark.init("/home/viktorgs/spark-2.4.5-bin-hadoop2.7")

from pyspark import SparkContext, SparkConf


reviewersPath = "./Data/yelp_top_reviewers_with_reviews.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)


reviewersTextFile = sc.textFile(reviewersPath)

# Subtask a) How many distinct users are there in the dataset
def distinctUsers(reviewersTextFile):
    return reviewersTextFile.map(lambda line: line.split()[1]).distinct().count()

# Subtask b) What is the average number of the characters in a user review
def averageCharactersInReviews(reviewersTextFile):
    reviews = reviewersTextFile.map(lambda line: line.split()[3]).filter(lambda column: column != u'"review_text"')
    numberOfReviews = reviews.count()
    totalReviewsLength = reviews.map(lambda review: len(base64.b64decode(review))).reduce(lambda a, b: a + b)
    return totalReviewsLength / numberOfReviews

# Subtask c) What is the business_id of the top 10 businesses with the most number of reviews
def topTenBusinessesWithMostReviews(reviewersTextFile):
    reviewsPerBusiness = reviewersTextFile.map(lambda line: (line.split("\t")[2], 1)).reduceByKey(lambda a, b: a+b)
    topTenBusinesses = reviewsPerBusiness.takeOrdered(10, key = lambda x: -x[1])
    return topTenBusinesses

# Subtask d) Find the number of reviews per year
def timestampToYear(timestamp):
    return str(datetime.utcfromtimestamp(float(timestamp)))[0:4]

def numberofReviewsPerYear(reviewsTextFile):
    unixTimestampsWithHead = reviewsTextFile.map(lambda line: line.split()[4])
    head = unixTimestampsWithHead.first()
    unixTimestamps = unixTimestampsWithHead.filter(lambda timestamp: timestamp != head)
    reviewsPerYear = unixTimestamps.map(lambda stamp: (timestampToYear(stamp), 1)).reduceByKey(lambda a, b: a+b)
    return reviewsPerYear

# Subtask e) What is the time and date of the first and last review
def timeAndDateOfFirtAndLastReview(reviewsTextFile):
    headers = reviewsTextFile.first()
    textFile = reviewsTextFile.filter(lambda line: line != headers)
    reviewDates = textFile.map(lambda line: (line.split("\t")[4]))
    minTime = reviewDates.reduce(lambda time1, time2: time1 if time1<time2 else time2)
    maxTime = reviewDates.reduce(lambda time1, time2: time1 if time1>time2 else time2)
    return minTime, maxTime

# Subtask f) Calculate the PCC between the number of reviews by a user and the average number of
#            the characters in the users review.

def main():

    print("-------- (2a) Number of distinct users: --------")
    #numberOfDistinctUsers = distinctUsers(reviewersTextFile)
    #print(numberOfDistinctUsers) # 4522

    print("-------- (2b) Average number of characters in a user review: --------")
    #averageCharacters = averageCharactersInReviews(reviewersTextFile)
    #print(averageCharacters) # 857

    print("-------- (2c) Business_id of top 10 businesses with most reviews: --------")
    #topTenBusinesses = topTenBusinessesWithMostReviews(reviewersTextFile)
    #for business in topTenBusinesses:
    #    print(business[0], business[1])

    print("-------- (2d) Number of reviews per year: --------")
    #reviewsPerYear = numberofReviewsPerYear(reviewersTextFile)
    #print(reviewsPerYear.collect())

    print("-------- (2e) Time and date of the first and last review: --------")
    firstReview, lastReview = timeAndDateOfFirtAndLastReview(reviewersTextFile)
    utc1 = str(datetime.utcfromtimestamp(float(firstReview)))
    utc2 = str(datetime.utcfromtimestamp(float(lastReview)))
    print("First review date: ", utc1)
    print("Last review date: ", utc2)

if __name__ == "__main__":
    main()
