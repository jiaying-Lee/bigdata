#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3b-sql").config("spark.some.config.option", "some-value").getOrCreate()
all_trips = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
all_trips.createOrReplaceTempView("alltrip")


car_date_trip = spark.sql("SELECT _c0 AS medallion, date_format(_c3,\'yyyy-MM-dd HH:mm:ss\') AS pickup_datetime\
                    FROM alltrip \
                    GROUP BY medallion, pickup_datetime \
                    HAVING COUNT(*) >1\
                    ORDER BY medallion ASC, pickup_datetime ASC")
#car_date_trip.createOrReplaceTempView("date_trip")

#result = car_date_trip.filter(car_date_trip['repeat'] > 1)



car_date_trip.select(format_string("%s,%s",car_date_trip.medallion, date_format(car_date_trip.pickup_datetime, 'yyyy-MM-dd HH:mm:ss'))).write.save("task3b-sql.out",format="text")
