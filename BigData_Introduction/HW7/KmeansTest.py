"""KmeansTest.py"""
from numpy import array
from math import sqrt
import os

from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans, KMeansModel


# main loop
conf = SparkConf().setAppName("KmeansTest")
sc = SparkContext(conf = conf)

# Load and parse the data
logFile = "/user/a2017210938/kddcup_convert.data"  # Should be some file on your system
data = sc.textFile(logFile)
parsedData = data.map(lambda line: array([float(x) for x in line.split(',')]))

# Build the model (cluster the data)
K = 23
clusters = KMeans.train(parsedData, K, maxIterations=1000, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    # return sqrt(sum([x ** 2 for x in (point - center)]))
    return sum([x ** 2 for x in (point - center)])

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
print("Within Set Sum of Squared Error = " + str(WSSSE) + "\n")

# Save and load model
clusters.save(sc, "./KMeansModel")
sameModel = KMeansModel.load(sc, "./KMeansModel")

# output kmeans center
print("center number:%d" % len(sameModel.clusterCenters))       # KMeansModel.clusterCenters get 2-dim list
print("sum of cost:%.2f" % sameModel.computeCost(parsedData))
# print(os.path.split(os.path.realpath(__file__))[0])
# fnew = open('/user/a2017210938/cluster_center.txt', 'w')
# for th_ in range(len(sameModel.clusterCenters)):
#     out_str = ""
#     temp = sameModel.clusterCenters[th_]
#     for i in range(len(temp) - 1):
#         out_str = out_str + str(temp[i]) + ','
#
#     out_str = out_str + str(temp[-1]) + '\n'
#     fnew.write(out_str)

# fnew.close()
sc.stop()