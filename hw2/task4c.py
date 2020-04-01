#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
#agent_name(20),fare_amount(5)
# (agent_name,fare_amount)  -> (agent_name,total_fare_amount) ->take10
#output (medallion_type, total_trips, total_revenue, avg_tip_percentage)
c1 = line1.map(lambda x: (x[20],float(x[5]))).reduceByKey(add).sortByKey().sortBy(lambda x: x[1], False)
# get the top 10
top = sc.parallelize(c1.take(10)).map(lambda x: str(x[0]) + "," + "{0:.2f}".format(float(x[1])))
top.saveAsTextFile("task4c.out")
sc.stop()
