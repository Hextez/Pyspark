from pyspark import SparkContext
import datetime
from collections import Counter

sc = SparkContext()

avg = sc.textFile("/home/cc1/project3-dataset.csv").map(lambda line: (str(line.split(",")[0]),{str(line.split(",")[1]) : 1 })).reduceByKey(lambda a , b : dict(Counter(a)+Counter(b)))

top = avg.map(lambda line : (line[0],len(line[1].keys())))
print str(top.collect())

f = open("/home/cc1/OutputPergunta4.txt", "w")
for i in top.top(1, key = lambda x : x[1]):
        f.write("Quem comunicou com mais destinos, " + str(i)+"\n")
f.close()




