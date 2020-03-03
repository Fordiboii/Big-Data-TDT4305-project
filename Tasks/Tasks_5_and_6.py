import findspark
findspark.init("/home/viktorgs/spark-2.4.5-bin-hadoop2.7")

from pyspark import SparkContext, SparkConf
from pyspark.sql.context import SQLContext

businessPath = "./Data/yelp_businesses.csv"
reviewersPath = "./Data/yelp_top_reviewers_with_reviews.csv"
friendshipPath = "./Data/yelp_top_users_friendship_graph.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)

# Task 5

sqlContext = SQLContext(sc)
businessDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").option("delimiter", "\t").load(businessPath)
reviewersDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").option("delimiter", "\t").load(reviewersPath)
friendshipDF = sqlContext.read.format("com.databricks.spark.csv").option("header", "false").option("inferSchema", "true").option("delimiter", ",").load(friendshipPath)

# a) Load each file in the dataset into separate DataFrames.

businessDf = businessDF.toDF("business_id", "name", "address", "city", "state", "postal_code", "latitude", "longitude", "stars", "review_count", "categories")
reviewersDF = reviewersDF.toDF("review_id", "user_id", "business_id", "review_text", "review_date")
friendshipDF = friendshipDF.toDF("src_user_id", "dst_user_id")

businessDF.createOrReplaceTempView("businesses")
reviewersDF.createOrReplaceTempView("reviewers")
friendshipDF.createOrReplaceTempView("friendshipGraph")

# Just to show that task 5 a) works:
# sqlContext.sql("select count(*) from businesses").show()
# sqlContext.sql("select count(*) from reviewers").show()
# sqlContext.sql("select count(*) from friendshipGraph").show()


# Task 6

# a)

# b)

# c)

