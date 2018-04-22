"""KmeansTest.py"""
import numpy as np

from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans, KMeansModel


# main loop
conf = SparkConf().setAppName("KmeansTest")
sc = SparkContext(conf = conf)

# Load and parse the data
logFile = "/user/a2017210938/kddcup_convert.data"  # Should be some file on your system
data = sc.textFile(logFile)
parsedData = data.map(lambda line: np.array([float(x) for x in line.split(',')]))

# Build the model (cluster the data)
K = 23
clusters = KMeans.train(parsedData, K, maxIterations=1000, initializationMode="random")

# Evaluate clustering by computing Within Set Sum of Squared Errors
def error(point):
    center = clusters.centers[clusters.predict(point)]
    return sum([x ** 2 for x in (point - center)])

WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)

# Save and load model
clusters.save(sc, "./KMeansModel")
sameModel = KMeansModel.load(sc, "./KMeansModel")

# output kmeans center
print("center number:%d" % len(sameModel.clusterCenters))       # KMeansModel.clusterCenters get 2-dim list
print("sum of cost using mllib:%.2f" % sameModel.computeCost(parsedData))
print("Within Set Sum of Squared Error = " + str(WSSSE) + "\n")
for th_ in range(len(sameModel.clusterCenters)):
    print(sameModel.clusterCenters[th_])
prediction = sameModel.predict(parsedData)
cluster_counter = np.zeros(K)
for i in range(len(prediction)):
    cluster_counter[prediction[i]] += 1
for i in range(K):
    print("cluster " + str(i) + "has " + str(cluster_counter[i]) + "points")

sc.stop()