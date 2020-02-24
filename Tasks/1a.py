import findspark
findspark.init()

from pyspark import SparkContext, SparkConf

businesspath = "./Data/yelp_businesses.csv"
reviewerspath = "./Data/yelp_top_reviewers_with_reviews.csv"
friendshippath = "./Data/yelp_top_users_friendship_graph.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

businessTextFile = sc.textFile(businesspath)
businessLines = businessTextFile.map(lambda line: line.split("\t"))
businessLines.cache()

print("dette funka")
