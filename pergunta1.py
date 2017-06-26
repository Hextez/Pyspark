from pyspark import SparkContext
import datetime
from collections import Counter

sc = SparkContext()

avg = sc.textFile("/home/cc1/project3-dataset.csv").map(lambda line: (str(line.split(",")[0]),{datetime.datetime.fromtimestamp(float(line.split(",")[8])).strftime('%Y-%m-%d') : long(line.split(",")[6]) })).reduceByKey(lambda a , b : dict(Counter(a)+Counter(b)))

top = avg.map(lambda line : (line[0],sum(line[1].values())))
avgs = avg.map(lambda line :(line[0], sum(line[1].values())/len(line[1].keys())))

top_sender = top.top(1, key = lambda x: x[1])
#top_sender = str(sorted(top.collect(),key = lambda val : val[1])[-1])
ave_sender = avgs.top(1,key = lambda val : val[1])
#ave_sender = str(sorted(avgs.collect(),key = lambda val : val[1])[-1])

def toCSVLine(data):
  return ','.join(str(d) for d in data)

out1 = top_sender.map(toCSVLine)
out1.saveAsTextFile('/home/cc1/OutputPergunta1TopSender')

out2 = ave_sender.map(toCSVLine)
out2.saveAsTextFile('/home/cc1/OutputPergunta1TopSender')

