from pyspark import SparkContext

from pyspark import SparkContext
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql import *
import datetime
from pyspark.sql.functions import sum

sc = SparkContext()

infos = sc.textFile("/home/cc1/project3-dataset.csv")
source = infos.map(lambda line : (str(line.split(",")[0]), 1 ).reduceByKey(lambda a , b: a + b)
dest = infos.map(lambda line : (str(line.split(",")[1]),1 ).reduceByKey(lambda a , b : a + b)

total = source.union(dest).reduceByKey(lambda a , b: a + b)

total.top(30,key=lambda x : x[1])

 
#info = sc.textFile("/home/cc1/project3-dataset.csv").map(lambda line : (str(line.split(",")[0]),str(line.split(",")[1]), long(line.split(",")[6]), datetime.datetime.fromtimestamp(float(line.split(",")[8])).strftime('%Y-%m-%d'), datetime.datetime.fromtimestamp(float(line.split(",")[10])).strftime('%Y-%m-%d')))
#info = sc.textFile("/home/cc1/project3-dataset.csv").map(lambda line : ((str(line.split(",")[0]),str(line.split(",")[1])), long(line.split(",")[6]))).reduceByKey(lambda a, b : a + b ).map(lambda line : (line[0][0],line[0][1],line[1]))

#spark = SparkSession(sc)
#sqlContext = SQLContext(spark);
#hasattr(rdd, "toDF")
## True

#dataframe = info.toDF(['source1','dest1', 'bytes1', 'timeStart1', 'timeEnd1'])
#dataframe2 = info.toDF(['source2','dest2', 'bytes2', 'timeStart2', 'timeEnd2'])

#dataframe = info.toDF(['source1','dest1', 'bytes1'])
#dataframe2 = info.toDF(['source2','dest2', 'bytes2'])


#data3 = dataframe.join(dataframe2, dataframe.dest1 == dataframe2.source2)
#dddd = data3.filter('source1 != dest2').filter('bytes1 = bytes2').filter('timeEnd1 == timeStart2').groupBy(['source1', 'dest1', 'dest2', 'timeEnd1']).agg(sum("bytes1").alias('bytes_sum'))
#dddd.show()
#rdd = data3.rdd
#print str(rdd.collect())
#rddReduce = rdd.map(lambda row : ((str(row['source1']),str(row['dest1']),str(row['dest2'])), long(row['bytes_sum'])))
#print str(rddReduce.collect())

