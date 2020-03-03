import findspark
findspark.init("/home/viktorgs/spark-2.4.5-bin-hadoop2.7")

from pyspark import SparkContext, SparkConf

businessPath = "./Data/yelp_businesses.csv"
reviewersPath = "./Data/yelp_top_reviewers_with_reviews.csv"
friendshipPath = "./Data/yelp_top_users_friendship_graph.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

businessTextFile = sc.textFile(businessPath)
reviewersTextFile = sc.textFile(reviewersPath)
friendshipTextFile = sc.textFile(friendshipPath)

# Subtask a)
businessLines = businessTextFile.map(lambda line: line.split("\t"))
reviewersLines = reviewersTextFile.map(lambda line: line.split("\t"))
friendshipLines = friendshipTextFile.map(lambda line: line.split(","))

print("Businesslines:", businessLines.count()) #        192 610
print("Reviewerslines:", reviewersLines.count()) #      883 738
print("Friendshiplines:", friendshipLines.count()) #    1 938 473
