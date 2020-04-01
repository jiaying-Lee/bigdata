#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader


head = "medallion"
def delhead(line):
    if head in line:
        return 1
    return 0

sc = SparkContext()

# read first file that first in the commend line --trips
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
# delete the header
# header1 = line1.first()
# line1 = line1.filter(lambda x: x != header1)
line1 = line1.filter(lambda line: delhead(line) != 1)

# read second file that second in the commend line --fares
line2 = sc.textFile(sys.argv[2])
line2 = line2.mapPartitions(lambda x: reader(x))
# delete the header
# header2 = line2.first()
# line2 = line2.filter(lambda x: x != header2)
line2 = line2.filter(lambda line: delhead(line) != 1)

# map the file
trips_map = line1.map(lambda x: ((x[0],x[1],x[2],x[5]),(x[3],x[4],x[6],x[7],x[8],x[9],x[10],x[11],x[12],x[13])))
fares_map = line2.map(lambda x: ((x[0],x[1],x[2],x[3]),(x[4],x[5],x[6],x[7],x[8],x[9],x[10])))

# inner join
# sortby(,ascending default = true,)
result = trips_map.join(fares_map).sortBy(lambda x: (x[0][0],x[0][1],x[0][3]))
output = result.map(lambda x: ','.join(key for key in x[0]) + ',' + ','.join(value for value in x[1][0]) + ',' + ','.join(value for value in x[1][1]))
output.saveAsTextFile('task1a.out')
sc.stop()

