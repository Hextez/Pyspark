from pyspark import SparkContext
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql import *



sc = SparkContext()

info = sc.textFile("/home/cc1/project3-dataset.csv").map(lambda line : ((str(line.split(",")[0]),str(line.split(",")[1])), long(line.split(",")[6])) if abs(hash(str(line.split(",")[0]))) < abs(hash(str(line.split(",")[1]))) else ((str(line.split(",")[1]),str(line.split(",")[0])), long(line.split(",")[6]))).reduceByKey(lambda a , b : a + b)


def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = info.map(toCSVLine)
lines.saveAsTextFile('/home/cc1/OutputPergunta2')
