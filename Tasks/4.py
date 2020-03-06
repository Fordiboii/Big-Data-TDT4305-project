import findspark

findspark.init("/home/viktorgs/spark-2.4.5-bin-hadoop2.7")

from pyspark import SparkContext, SparkConf


friendshipPath = "./Data/yelp_top_users_friendship_graph.csv"

sparkConf = SparkConf().setAppName("Yelp").setMaster("local")
sc = SparkContext(conf = sparkConf)


friendshipTextFile = sc.textFile(friendshipPath)


def nodeFrequencies(textFile):
    headers = textFile.first()
    textFileWithoutHeaders = textFile.filter(lambda line: line != headers)
    nodes = textFileWithoutHeaders.map(lambda line: str(line)).map(lambda line: line.split(","))
    srcNodeFrequencies = nodes.map(lambda nodes: nodes[0]).map(lambda node: (node, 1)).reduceByKey(lambda a,b: a+b)
    destNodeFrequencies = nodes.map(lambda nodes: nodes[1]).map(lambda node: (node, 1)).reduceByKey(lambda a,b: a+b)
    return srcNodeFrequencies, destNodeFrequencies

# Subtask a)
def topTenNodes(textFile):
    srcNodeFrequencies, destNodeFrequencies = nodeFrequencies(textFile)
    topTenSrcNodes = srcNodeFrequencies.takeOrdered(10, key = lambda x: -x[1])
    topTenDestNodes = destNodeFrequencies.takeOrdered(10, key = lambda x: -x[1])
    return topTenSrcNodes, topTenDestNodes

# Subtask b)
def meanDegrees(textFile):
    srcNodeFrequencies, destNodeFrequencies = nodeFrequencies(textFile)
    srcCountList = srcNodeFrequencies.map(lambda tuple: tuple[1])
    destCountList = destNodeFrequencies.map(lambda tuple: tuple[1])
    totalDistinctSrcNodes = srcCountList.count()
    totalDistinctDestNodes = destCountList.count()
    totalOutDegrees = srcCountList.reduce(lambda a, b: a+b)
    totalInDegrees = destCountList.reduce(lambda a, b: a+b)
    return str(round(float(float(totalOutDegrees)/float(totalDistinctSrcNodes)), 2)), str(round(float(float(totalInDegrees)/float(totalDistinctDestNodes)), 2))

def findMedian(srcCount, srcList, destCount, destList):
    if ((srcCount-1) % 2) == 0:
        srcNodesMedian = (srcList.lookup((srcCount-1)/2)[0] + srcList.lookup(((srcCount-1)/2)+1)[0]) /2
    else:
        srcNodesMedian = srcList.lookup((srcCount)/2)[0]

    if ((destCount-1) % 2) == 0:
        destNodesMedian = (destList.lookup((destCount-1)/2)[0] + destList.lookup(((destCount-1)/2)+1)[0]) / 2
    else:
        destNodesMedian = destList.lookup(destCount/2)[0]
    return srcNodesMedian, destNodesMedian

def medianDegrees(textFile):
    srcNodeFrequencies, destNodeFrequencies = nodeFrequencies(textFile)
    srcCountList = srcNodeFrequencies.map(lambda tuple: tuple[1])
    destCountList = destNodeFrequencies.map(lambda tuple: tuple[1])
    totalDistinctSrcNodes = srcCountList.count()
    totalDistinctDestNodes = destCountList.count()
    sortedSrcCountList = srcCountList.sortBy(lambda a: a)
    sortedDestCountList = destCountList.sortBy(lambda a: a)
    indexedSrcList = sortedSrcCountList.zipWithIndex().map(lambda tuple: (tuple[1], tuple[0]))
    indexedDestList = sortedDestCountList.zipWithIndex().map(lambda tuple: (tuple[1], tuple[0]))
    return findMedian(totalDistinctSrcNodes, indexedSrcList, totalDistinctDestNodes, indexedDestList)

def main():
    print("-------- (4a) Top 10 nodes with in and out degress --------")
    #srcNodes, destNodes = topTenNodes(friendshipTextFile)
    #print("Top 10 src nodes (most out-degrees)")
    #for node in srcNodes:
    #    print(node)
    
    #print("Top 10 destination nodes (most in-degrees)")
    #for node in destNodes:
    #    print(node)

    print("-------- (4b) Mean and median for in and out degrees --------")
    srcNodesMean, destNodesMean = meanDegrees(friendshipTextFile)
    #srcNodesMedian, destNodesMedian = medianDegrees(friendshipTextFile)
    print("Average out-degree: " ,srcNodesMean) # 12
    print("Average inn-degree: " ,destNodesMean) # 3
    #print("Median out-degree: ", srcNodesMedian) # 1
    #print("Median inn-degree: ", destNodesMedian) # 1

if __name__ == "__main__":
    main()