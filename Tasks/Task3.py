import findspark
import base64

findspark.init("/home/viktorgs/spark-2.4.5-bin-hadoop2.7")

from pyspark import SparkContext, SparkConf


businessPath = "./Data/yelp_businesses.csv"
sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)


businessTextFile = sc.textFile(businessPath)

# Subtask a) What is the average rating for businesses in each city?

# Subtask b) What are the top 10 most frequent categories in the data?
def mostFrequentCategories(textFile):
    headers = textFile.first()
    textFileWithoutHeaders = textFile.filter(lambda line: line != headers)
    arrayOfCategories = textFileWithoutHeaders.map(lambda line: line.split("\t")[10:])
    listOfCategoriesUnix = arrayOfCategories.flatMap(lambda list: list)
    arrayOfCategoriesAscii = listOfCategoriesUnix.map(lambda category: str(category)).map(lambda category: category.split(","))
    listOfCategoriesAscii = arrayOfCategoriesAscii.flatMap(lambda list: list)
    categoryFrequencies = listOfCategoriesAscii.map(lambda category: (category, 1)).reduceByKey(lambda a,b: a+b)
    topTenCategories = categoryFrequencies.takeOrdered(10, key = lambda x: -x[1])
    return topTenCategories


# Subtask c) 


def main():
    print("-------- (3a) Average rating for businesses in each city: --------")


    print("-------- (3b) Top 10 most frequent categories: --------")
    categories = mostFrequentCategories(businessTextFile)
    for category in categories:
        print(category[0], category[1])

    print("-------- (3c) lala: --------")


if __name__ == "__main__":
    main()
