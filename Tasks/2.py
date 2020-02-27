import findspark
import base64
findspark.init("/home/fordiboii/spark")

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
def distinctBusinesses(rdd):
    newRdd = rdd.map(lambda x: x.split()[2]).distinct()
    return newRdd.count()

reviewersTextFile = sc.textFile(reviewerspath)

print("distinct users: " + str(distinctUsers(reviewersTextFile)))

print("distinct businesses: " + str(distinctBusinesses(reviewersTextFile)))


