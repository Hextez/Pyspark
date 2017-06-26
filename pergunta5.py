from pyspark import SparkContext

sc = SparkContext()

file = sc.textFile("/home/cc1/project3-dataset.csv")
ipSender = file.map(lambda line: (str(line.split(",")[0]), long(line.split(",")[6]))).reduceByKey(lambda a , b : a+b)
ipRecv = file.map(lambda line: (str(line.split(",")[1]), long(line.split(",")[6]))).reduceByKey(lambda a , b : a+b)


top_sender = ipSender.top(1, key = lambda x: x[1])
#top_sender = str(sorted(top.collect(),key = lambda val : val[1])[-1])
top_recv = ipRecv.top(1,key = lambda val : val[1])
#ave_sender = str(sorted(avgs.collect(),key = lambda val : val[1])[-1])

f = open("/home/cc1/OutputPergunta5.txt", "w")
for i in top_sender:
        f.write("Quem enviou mais, " + str(i)+"\n")
for i in top_recv:
        f.write("Quem recebeu mais, "+ str(i)+"\n")
f.close()

