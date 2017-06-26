from pyspark import SparkContext
from collections import Counter
import datetime



sc = SparkContext()

info = sc.textFile("/home/cc1/project3-dataset.csv").map(lambda line : ((str(line.split(",")[0]),str(line.split(",")[1])), {datetime.datetime.fromtimestamp(float(line.split(",")[8])).strftime('%Y-%m-%d') : 1 }) if abs(hash(str(line.split(",")[0]))) < abs(hash(str(line.split(",")[1]))) else ((str(line.split(",")[1]),str(line.split(",")[0])), {datetime.datetime.fromtimestamp(float(line.split(",")[8])).strftime('%Y-%m-%d') : 1 })).reduceByKey(lambda a , b : dict(Counter(a) + Counter(b)))


strassa = info.map(lambda line :(line[0], sum(line[1].values())/len(line[1].keys())))

def toCSVLine(data):
  return ','.join(str(d) for d in data)

lines = strassa.map(toCSVLine)
lines.saveAsTextFile('/home/cc1/OutputPergunta3')


