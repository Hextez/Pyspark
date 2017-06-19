from pyspark import SparkContext
from collections import Counter
from pyspark.sql import SparkSession
from pyspark.sql import *

def tupls(a):
	return (a[0][0],a[0][1],a[1])

sc = SparkContext()

info = sc.textFile("/home/merda/Downloads/project3-dataset.csv").map(lambda line : ((str(line.split(",")[0]),str(line.split(",")[1])), long(line.split(",")[6])) if abs(hash(str(line.split(",")[0]))) < abs(hash(str(line.split(",")[1]))) else ((str(line.split(",")[1]),str(line.split(",")[0])), long(line.split(",")[6]))).reduceByKey(lambda a , b : a + b)

#info.collect()

def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = info.map(toCSVLine)
lines.saveAsTextFile('labels-and-predictions.csv')


#spark = SparkSession(sc)
#sqlContext = SQLContext(spark);
#hasattr(rdd, "toDF")
## True

#dataframe = cal.toDF(['source','dest', 'bytes'])
#dataframe2 = cal.toDF(['source1', 'dest1', 'bytes1'])
#interactions_df = sqlContext.createDataFrame(dataframe)
#interactions_df.registerTempTable("coisa")

#data3 = dataframe.join(dataframe2, dataframe.source == dataframe2.source1)
#data3.where("source = dest1 and dest = source1").show()

#dataframe.registerTempTable("coisa1")
#dataframe2.registerTempTable("coisa2")
#ss = sqlContext.sql("""SELECT source, dest, sum(bytes) as byt FROM coisa1, coisa2 WHERE source = dest1 and dest = source1 """)##.show(False) ##UNION SELECT source, dest, sum(bytes) FROM coisa1, coisa2 WHERE source != dest1 and dest = source1""").show()
#ss.show(ss.count(),False)
