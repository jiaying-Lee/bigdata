#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task2a-sql").config("spark.some.config.option", "some-value").getOrCreate()
all_trips = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
all_trips.createOrReplaceTempView("alltrip")

# range1 = spark.sql("SELECT COUNT(summons_number) AS weekend\
#                      FROM park\
#                      WHERE ")
# range1.createOrReplaceTempView("tmp1")

result = spark.sql("SELECT t1.p1 AS num1,t2.p2 AS num2,t3.p3 AS num3,t4.p4 AS num4,t5.p5 AS num5,t6.p6 AS num6\
                  FROM\
                  ((SELECT COUNT(_c15) AS p1 FROM alltrip WHERE _c15>=0 and _c15 <=5) t1 CROSS JOIN\
                  (SELECT COUNT(_c15) AS p2 FROM alltrip where _c15>5 and _c15 <=15) t2 CROSS JOIN\
                  (SELECT COUNT(_c15) as p3 FROM alltrip where _c15>15 and _c15 <=30) t3 CROSS JOIN\
                  (SELECT COUNT(_c15) as p4 FROM alltrip where _c15>30 and _c15 <=50) t4 CROSS JOIN\
                  (SELECT COUNT(_c15) as p5 FROM alltrip where _c15>50 and _c15 <=100) t5 CROSS JOIN\
                  (SELECT COUNT(_c15) as p6 FROM alltrip where _c15>100) t6 )")

#or use CASE WHEN
result.select(format_string('0,5,%s\n5,15,%s\n15,30,%s\n30,50,%s\n50,100,%s\n100,infinite,%s',
                            result.num1, result.num2, result.num3,result.num4,result.num5,result.num6)).write.save("task2a-sql.out", format="text")