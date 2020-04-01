import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
count = line1.map(lambda x: (x[7],1)).reduceByKey(add).sortByKey().map(lambda x: x[0] + "," + str(x[1])).saveAsTextFile("task2b.out")
sc.stop()