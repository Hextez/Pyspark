from pyspark import SparkContext
from collections import Counter
import datetime

def hashIPS(ip,ip2):
	if abs(hash(ip)) < abs(hash(ip2)):
		return ((ip,ip2), 1)
	else:
		return ((ip2,ip), 1)

sc = SparkContext()

info = sc.textFile("/home/cc1/project3-dataset.csv")

comm = info.map(lambda line : hashIPS(str(line.split(",")[0]),str(line.split(",")[1]))).reduceByKey(lambda a, b : a + b)

days = info.map( lambda line : (datetime.datetime.fromtimestamp(float(line.split(",")[8])).strftime('%Y-%m-%d') , 1)).reduceByKey(lambda a , b : a+b)

numeroDias = days.count()

strassa = comm.map(lambda line : ((line[0][0],line[0][1]),line[1]/numeroDias))

def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = strassa.map(toCSVLine)
lines.saveAsTextFile('/home/cc1/OutputPergunta3')


