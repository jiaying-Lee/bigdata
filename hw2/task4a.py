#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
#vehicle_type(16),fare_amount(5),tip_amount(8)
# (vehicle_type,(fare_amount,tip_amount/fare_amount,1)  -> (vehicle_type,(total_fare_amount,total_tip_amount/fare_amount,total_trip)
#output (vehicle_type, total_trips, total_revenue, avg_tip_percentage)
c1 = line1.map(lambda x: (x[16],(x[5],float(x[8])/float(x[5]) if float(x[5]) != 0 else 0,1))).reduceByKey(lambda x,y : (float(x[0])+float(y[0]),float(x[1])+float(y[1]),float(x[2])+float(y[2])))
output = c1.map(lambda x: x[0] + "," + str(int(x[1][2])) + "," + "{0:.2f}".format(float(x[1][0])) + "," + "{0:.2f}".format(float(x[1][1])/float(x[1][2])*100)).sortBy(lambda x: x[0])
output.saveAsTextFile("task4a.out")
sc.stop()
