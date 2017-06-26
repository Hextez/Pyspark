from pyspark import SparkContext

from pyspark import SparkContext
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql import *
import datetime
from pyspark.sql.functions import sum

sc = SparkContext()

infos = sc.textFile("/home/cc1/project3-dataset.csv")
source = infos.map(lambda line : (str(line.split(",")[0]), 1 )).reduceByKey(lambda a , b: a + b)
dest = infos.map(lambda line : (str(line.split(",")[1]),1 )).reduceByKey(lambda a , b : a + b)

total = source.union(dest).reduceByKey(lambda a , b: a + b)

gateways = total.top(30,key=lambda x : x[1])

def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = gateways.map(toCSVLine)
lines.saveAsTextFile('/home/cc1/OutputPergunta6')