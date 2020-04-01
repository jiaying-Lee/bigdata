#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
#pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude [10]-[13]
# c1 = (medallion,1,1 if allzero else 0)  (taxi,total_trip,total_noGPS)  (count how many trips per taxi)
#output = (medallion, total_noGPS/total_trip)

c1 = line1.map(lambda x: (x[0], (1,(1 if (float(x[10])== 0 and float(x[11])== 0 and float(x[12])== 0 and float(x[13])== 0) else 0))))
#c1 = c1.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).filter(lambda x: x[1][1] > 0).sortByKey()
c1 = c1.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1])).sortByKey()
output = c1.map(lambda x: x[0] + "," + "{0:.2f}".format(float(x[1][1])/float(x[1][0]))).sortBy(lambda x: x[0]).saveAsTextFile("task3c.out")
sc.stop()