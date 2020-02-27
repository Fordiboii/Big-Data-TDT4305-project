import findspark
findspark.init("/home/fordiboii/spark")

from pyspark import SparkContext, SparkConf

reviewerspath = "./data/yelp_top_reviewers_with_reviews.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

"""2 a) How many distinct users are there in the dataset """
def distinctUsers(rdd):
    newRdd = rdd.map(lambda x: x.split()[1]).distinct()
    return newRdd.count()

reviewersTextFile = sc.textFile(reviewerspath)

print(distinctUsers(reviewersTextFile))