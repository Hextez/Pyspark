from pyspark import SparkContext

sc = SparkContext()

#tops = sc.textFile("/home/merda/Downloads/project3-dataset.csv").map(lambda line: (str(line.split(",")[0]),long(line.split(",")[6]))).reduceByKey(lambda a , b : a+b)
avg = sc.textFile("/home/merda/Downloads/project3-dataset.csv").map(lambda line: (str(line.split(",")[0]), long(line.split(",")[6])).reduceByKey(lambda a , b : a+b)
#top = avg.map(lambda line : (line[0],sum(line[1].values())))
print "top guy sender "+str(sorted(avg.collect(),key = lambda val : val[1])[-1])

avg1 = sc.textFile("/home/merda/Downloads/project3-dataset.csv").map(lambda line: (str(line.split(",")[1]), long(line.split(",")[6])).reduceByKey(lambda a , b : a+b)
print "top guy recv "+str(sorted(avg1.collect(),key = lambda val : val[1])[-1])



