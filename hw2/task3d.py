#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
#(value)medallion key[0] (key)licence [1]
#((licence,medallion),1)  --> c2(licence,1)) -->(licence,(medallion,total_medallion))
c1 = line1.map(lambda x: ((x[1],x[0]),1)).reduceByKey(add)
#for row in c1.take(10): print(row)
#c2 ('CC05618EFA21B1EF07D8337B2F488858', ('00005007A9F30E289E760362F69E4EAD', 1))
c2 = c1.map(lambda x: (x[0][0],1)).reduceByKey(add).sortBy(lambda x: x[0])
#for row in c2.take(10): print(row)
output = c2.map(lambda x: x[0] + "," + str(x[1])).saveAsTextFile("task3d.out")
sc.stop()