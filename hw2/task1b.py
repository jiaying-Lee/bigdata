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
# read first file that first in the commend line --fares
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
# delete the header
line1 = line1.filter(lambda line: delhead(line) != 1)

# read second file that second in the commend line --licenses
line2 = sc.textFile(sys.argv[2])
line2 = line2.mapPartitions(lambda x: reader(x))
# delete the header
line2 = line2.filter(lambda line: delhead(line) != 1)

# map the file
fares_map = line1.map(lambda x: (x[0], (x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10])))
licenses_map = line2.map(
    lambda x: (x[0], (x[1], x[2], x[3], x[4], x[5], x[6], x[7], x[8], x[9], x[10], x[11], x[12], x[13], x[14], x[15])))
# inner join
# sortby(,ascending default = true,)
result = fares_map.join(licenses_map).sortBy(lambda x: (x[0], x[1][0][0], x[1][0][2]))
output = result.map(lambda x: x[0] + ',' + ','.join(value for value in x[1][0]) + ',' + '\"' + x[1][1][0] + '\"' + ',' + ','.join(value for value in x[1][1][1:]))

output.saveAsTextFile('task1b.out')
sc.stop()
