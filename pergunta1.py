from pyspark import SparkContext
import datetime
from collections import Counter

sc = SparkContext()
info = sc.textFile("/home/cc1/project3-dataset.csv")

totalInfo = info.map(lambda line: (str(line.split(",")[0]),long(line.split(",")[6]))).reduceByKey(lambda a , b : a + b)
days = info.map(lambda line : (datetime.datetime.fromtimestamp(float(line.split(",")[8])).strftime('%Y-%m-%d') , 1 )).reduceByKey(lambda a , b : a + b)

top = totalInfo.top(1 , key = lambda x : x[1])
numeroDias = days.count()

avg = totalInfo.map( lambda x : (x[0],x[1]/numeroDias))
ave_sender = avg.top(1,key = lambda val : val[1])

f = open("/home/cc1/OutputPergunta1.txt", "w")
for i in top:
	f.write("Quem enviou mais, " + str(i)+"\n")
for i in ave_sender:
	f.write("A sua media por dias, "+ str(i)+"\n")
f.close()


