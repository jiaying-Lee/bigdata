#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
output = line1.map(lambda x: ('<0', (1 if float(x[-6]) < 0 else 0))).reduceByKey(add).map(lambda x: str(x[1])).saveAsTextFile("task3a.out")
sc.stop()