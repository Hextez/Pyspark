from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD
import datetime
from collections import Counter

sc = SparkContext()

#tops = sc.textFile("/home/merda/Downloads/project3-dataset.csv").map(lambda line: (str(line.split(",")[0]),long(line.split(",")[6]))).reduceByKey(lambda a , b : a+b)
avg = sc.textFile("/home/merda/Downloads/project3-dataset.csv").map(lambda line: (str(line.split(",")[0]),{str(line.split(",")[0]) : 1 }).reduceByKey(lambda a , b : a)

top = avg.map(lambda line : (line[0],len(line[1].keys())))

print "top guy "+str(sorted(top.collect(),key = lambda val : val[1])[-1])



