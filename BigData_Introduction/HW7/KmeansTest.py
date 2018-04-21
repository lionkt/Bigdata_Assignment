"""KmeansTest.py"""
from numpy import array
from math import sqrt

from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import KMeans, KMeansModel


# main loop
# logFile = "/user/a2017210938/kddcup_convert.data_10_percent"  # Should be some file on your system
logFile = "/user/a2017210938/README.md"  # Should be some file on your system

conf = SparkConf().setAppName("KmeansTest")
sc = SparkContext(logFile)

# # Load and parse the data
# data = sc.textFile()
# parsedData = data.map(lambda line: array([float(x) for x in line.split(',')]))
#
# # Build the model (cluster the data)
# K = 23
# clusters = KMeans.train(parsedData, K, maxIterations=10, initializationMode="random")
# Evaluate clustering by computing Within Set Sum of Squared Errors
# def error(point):
#     center = clusters.centers[clusters.predict(point)]
#     return sqrt(sum([x ** 2 for x in (point - center)]))
#
# WSSSE = parsedData.map(lambda point: error(point)).reduce(lambda x, y: x + y)
# print("Within Set Sum of Squared Error = " + str(WSSSE))
#
# # Save and load model
# # clusters.save(sc, "./KMeansModel")
# # sameModel = KMeansModel.load(sc, "./KMeansModel")
#
# sc.stop()


logData = sc.textFile(logFile).cache()
numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()
print("Lines with a: %i, lines with b: %i" % (numAs, numBs))