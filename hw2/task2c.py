import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
count = line1.map(lambda x: (x[3][:10],((float(x[-6])+float(x[-3])+float(x[-5])),float(x[-2]))))
count = count.reduceByKey(lambda x,y: (float(x[0]) + float(y[0]), float(x[1]) + float(y[1]))).sortByKey()
count = count.map(lambda x: x[0] + "," + "{0:.2f}".format(float(x[1][0])) + "," + "{0:.2f}".format(float(x[1][1]))).saveAsTextFile("task2c.out")
sc.stop()