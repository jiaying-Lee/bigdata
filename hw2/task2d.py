import sys
from pyspark import SparkContext
from csv import reader
from operator import add

sc = SparkContext()
line1 = sc.textFile(sys.argv[1])
line1 = line1.mapPartitions(lambda x: reader(x))
#day -- pickup_datetime[3][:10]
# c1 = ((medallion,DATE),1)    (count how many trips per day per car)
# the output of c1 (medallion,date,day_amount)==> (medallion,1,day_amount) (count how many days)
# (medallion,(date,1))
c1 = line1.map(lambda x: ((x[0],x[3][:10]),1)).reduceByKey(add).sortBy(lambda x: (x[0][0],x[0][1]))
#TypeError: 'PipelinedRDD' object is not iterable ==> no parallelize but map
c2 = c1.map(lambda x: (x[0][0],(1,x[1]))).reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1])).sortByKey()
# medallion, total_trip, total_day, average
output = c2.map(lambda x: x[0] + "," + str(x[1][1]) + "," + str(x[1][0]) + "," + "{0:.2f}".format(float(x[1][1])/float(x[1][0])))
output.saveAsTextFile("task2d.out")
sc.stop()