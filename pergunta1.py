from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.classification import LogisticRegressionWithSGD
import datetime
from collections import Counter

sc = SparkContext()

#tops = sc.textFile("/home/merda/Downloads/project3-dataset.csv").map(lambda line: (str(line.split(",")[0]),long(line.split(",")[6]))).reduceByKey(lambda a , b : a+b)
avg = sc.textFile("/home/merda/Downloads/project3-dataset.csv").map(lambda line: (str(line.split(",")[0]),{datetime.datetime.fromtimestamp(float(line.split(",")[8])).strftime('%Y-%m-%d') : long(line.split(",")[6]) })).reduceByKey(lambda a , b : dict(Counter(a)+Counter(b)))

top = avg.map(lambda line : (line[0],sum(line[1].values())))
avgs = avg.map(lambda line :(line[0], sum(line[1].values())/len(line[1].keys())))

print "top guys "+ str(sorted(top.collect(),key = lambda val : val[1])[-1])
print "average guy "+str(sorted(avgs.collect(),key = lambda val : val[1])[-1])



