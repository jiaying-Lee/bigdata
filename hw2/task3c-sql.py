#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3c-sql").config("spark.some.config.option", "some-value").getOrCreate()
all_trips = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
all_trips.createOrReplaceTempView("alltrip")

#pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude [10]-[13]
gps0 = spark.sql("SELECT _c0 AS medallion, COUNT(*) AS zero_count\
                    FROM alltrip \
                    WHERE ( _c10 = 0 AND _c11 = 0 AND _c12 = 0 AND _c13 = 0)\
                    GROUP BY medallion\
                    ORDER BY medallion ASC")

gps0.createOrReplaceTempView("gps")

total_days = spark.sql("SELECT _c0 AS medallion, COUNT(*) AS days_driven \
                       FROM alltrip\
                       GROUP BY medallion\
                       ORDER BY medallion ASC")

total_days.createOrReplaceTempView("days")

result = spark.sql("SELECT gps.zero_count, days.* FROM  days LEFT OUTER JOIN gps ON gps.medallion = days.medallion ORDER BY medallion ASC")
result.createOrReplaceTempView("res")
output = spark.sql("SELECT medallion , (nvl(zero_count,0)/days_driven) AS percentage_of_trips FROM res ORDER BY medallion ASC")
output.select(format_string("%s,%.2f",output.medallion,output.percentage_of_trips)).write.save("task3c-sql.out",format="text")
