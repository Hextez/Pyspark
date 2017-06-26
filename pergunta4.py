from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD
import datetime
from collections import Counter

sc = SparkContext()

avg = sc.textFile("/home/cc1/project3-dataset.csv").map(lambda line: (str(line.split(",")[0]),{str(line.split(",")[0]) : 1 })).reduceByKey(lambda a , b : a)

top = avg.map(lambda line : (line[0],len(line[1].keys()))).top(1, key = lambda x : x[1])

def toCSVLine(data):
  return ','.join(str(d) for d in data)

out1 = top.map(toCSVLine)
out1.saveAsTextFile('/home/cc1/OutputPergunta4')



