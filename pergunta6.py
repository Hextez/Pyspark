from pyspark import SparkContext

from pyspark import SparkContext
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql import *

sc = SparkContext()

#info = sc.textFile("/home/merda/Downloads/project3-dataset.csv").map(lambda line : (str(line.split(",")[0]),str(line.split(",")[1]), long(line.split(",")[6]), float(line.split(",")[8]), float(line.split(",")[10])))
info = sc.textFile("/home/merda/Downloads/project3-dataset.csv").map(lambda line : ((str(line.split(",")[0]),str(line.split(",")[1])), long(line.split(",")[6]))).reduceByKey(lambda a, b : a + b ).map(lambda line : (line[0][0],line[0][1],line[1]))

spark = SparkSession(sc)
#sqlContext = SQLContext(spark);
#hasattr(rdd, "toDF")
## True

#dataframe = info.toDF(['source1','dest1', 'bytes1', 'timeStart1', 'timeEnd1'])
#dataframe2 = info.toDF(['source2','dest2', 'bytes2', 'timeStart2', 'timeEnd2'])

dataframe = info.toDF(['source1','dest1', 'bytes1'])
dataframe2 = info.toDF(['source2','dest2', 'bytes2'])


data3 = dataframe.join(dataframe2, dataframe.dest1 == dataframe2.source2)

data3.show()
rdd = data3.rdd

rddReduce = rdd.filter(lambda line : line[0] != line[4] and line[2] == line[5])

print str(rddReduce.collect())

