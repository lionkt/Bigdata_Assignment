"""SimpleApp.py"""
from pyspark import SparkContext,SparkConf

# logFile = "/user/qinwei/README.md"  # Should be some file on your system
logFile = "/user/a2017210938/README.md"  # Should be some file on your system


conf = SparkConf().setAppName("SimpleApp")
sc = SparkContext(conf = conf)
logData = sc.textFile(logFile).cache()

numAs = logData.filter(lambda s: 'a' in s).count()
numBs = logData.filter(lambda s: 'b' in s).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))
