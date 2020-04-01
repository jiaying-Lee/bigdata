#!/usr/bin/env python
import sys
from pyspark import SparkContext
from csv import reader
from operator import add

# range = ['[0,5)','[5,15)','[15,30)','[30,50)','[50,100)','[>100)']

head = "fault"
def delhead(line):
    if head in line:
        return 1
    return 0

def maptorange(x):
    if 0 <= float(x) <= 5:
        return (0,5)
    elif 5 < float(x) <= 15:
        return (5,15)
    elif 15 < float(x) <= 30:
        return (15,30)
    elif 30 < float(x) <= 50:
        return (30,50)
    elif 50 < float(x) <= 100:
        return (50,100)
    elif 100 < float(x):
        return (100,'infinite')
    else:
        return 'fault'



sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
# #map to range
# attention: cannot pass x or x[-6] into maptorange, but only float(x[-6])
range = line1.map(lambda x: (maptorange(float(x[-6])),1)).reduceByKey(add)
# value = line1.flatMap(lambda x: x[-6]).map(lambda x: str(maptorange(x)),1).reduceByKey(lambda x, y: x + y).sortByKey()
# value.saveAsTextFile("task2a.out")

#delete 'nan'
#ret1 = re.search('\d+', str).group()
range = range.filter(lambda x: delhead(x) != 1).sortBy(lambda x: x[0]).map(lambda x: "%s,%s,%s"%(x[0][0],x[0][1],x[1])).saveAsTextFile("task2a.out")
# # weekend is 5,6,12,13,19,20,26,27. and total 8 days
# weekend = line1.map(lambda x: (x[2],(1 if int(x[1][-2:]) in (5,6,12,13,19,20,26,27) else 0))).reduceByKey(lambda x, y: x + y).map(lambda x: (x[0], float(x[1])/8.0))
# # first fullouterjoin the weekday and weekend, output is key    weekend_average,weekday_average
# weekend.fullOuterJoin(weekday).map(lambda x: str(x[0]) + "\t" + "{0:.2f}".format(x[1][0]) + ", " + "{0:.2f}".format(x[1][1])).saveAsTextFile("task7.out")
# You can only append strings to strings using the "+" operator, not integers.

# range1 = line1.map(lambda x: ('[0,5)', (1 if 0 <= float(x[-6]) < 5 else 0))).reduceByKey(lambda x, y: x + y).map(lambda x: x[0] + "," + str(x[1])).saveAsTextFile("task7.out")
'''
range2 = line1.map(lambda x: ('[5,15)', (1 if 5 <= float(x[-6]) < 15 else 0))).reduceByKey(lambda x, y: x + y).map(lambda x: x[0] + "," + str(x[1])).saveAsTextFile("task7.out")
range3 = line1.map(lambda x: ('[15,30)', (1 if 15 <= float(x[-6]) < 30 else 0))).reduceByKey(lambda x, y: x + y).map(lambda x: x[0] + "," + str(x[1])).saveAsTextFile("task7.out")
range4 = line1.map(lambda x: ('[30,50)', (1 if 30 <= float(x[-6]) < 50 else 0))).reduceByKey(lambda x, y: x + y).map(lambda x: x[0] + "," + str(x[1])).saveAsTextFile("task7.out")
range5 = line1.map(lambda x: ('[50,100)', (1 if 50 <= float(x[-6]) < 100 else 0))).reduceByKey(lambda x, y: x + y).map(lambda x: x[0] + "," + str(x[1])).saveAsTextFile("task7.out")
range6 = line1.map(lambda x: ('[>100)', (1 if 100 <= float(x[-6]) else 0))).reduceByKey(lambda x, y: x + y).map(lambda x: x[0] + "," + str(x[1])).saveAsTextFile("task7.out")
'''

# range1 = line1.map(lambda x: ('[0,5)' if 0 <= float(x[-6]) < 5
#                               else ('[5,15)' if 5 <= float(x[-6]) < 15
#                                     else ('[15,30)' if 15 <= float(x[-6]) < 30
#                                           else ('[30,50)' if 30 <= float(x[-6]) < 50
#                                                 else ('[50,100)' if 50 <= float(x[-6]) < 100
#                                                       else ('[>100)' if 100 <= float(x[-6]) else ('nan')))))),
#                               1)).reduceByKey(lambda x, y: x + y).map(lambda x: x[0] + "," + str(x[1])).saveAsTextFile(
#     "task2a.out")
# result = map(range1,range2)
sc.stop()