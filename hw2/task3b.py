import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
#day -- pickup_datetime[3]
# c1 = ((medallion,DATEtime),1)    (count how many trips for the same car at the same time)
# the output of c1 (medallion,date,day_amount)==> (medallion,1,day_amount) (count how many days)
# (medallion,(date,1))
c1 = line1.map(lambda x: ((x[0],x[3]),1)).reduceByKey(add).sortBy(lambda x: (x[0][0],x[0][1]))
output = c1.filter(lambda x: x[1] > 1).map(lambda x: x[0][0] + "," + x[0][1]).saveAsTextFile("task3b.out")
sc.stop()