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

totals = source.union(dest)
print str(totals.collect())
total= totals.reduceByKey(lambda a , b: a + b)

gateways = total.top(30,key=lambda x : x[1])

f = open("/home/cc1/OutputPergunta6.txt", "w")
for i in gateways:
        f.write( str(i)+"\n")
f.close()

