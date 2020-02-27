import findspark
findspark.init("/home/olekfur/spark")

from pyspark import SparkContext, SparkConf

reviewerspath = "../data/yelp_top_reviewers_with_reviews.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

# 2 a) How many distinct users are there in the dataset
def distinctUsers(rdd):
    newRdd = rdd.map(lambda x: x.split()[1]).distinct()
    return newRdd.count()

# 2 c) What is the business_id of the top 10 businesses with the most reviews
def distinctBusinesses(rdd):
    newRdd = rdd.map(lambda x: x.split()[2]).distinct()
    return newRdd.count()

reviewersTextFile = sc.textFile(reviewerspath)

print("distinct users: " + str(distinctUsers(reviewersTextFile)))

print("distinct businesses: " + str(distinctBusinesses(reviewersTextFile)))