#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task2d-sql").config("spark.some.config.option", "some-value").getOrCreate()
all_trips = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
all_trips.createOrReplaceTempView("alltrip")


car_date_trip = spark.sql("SELECT _c0 AS medallion, date_format(_c3, \'yyyy-MM-dd\') AS pickup_date, COUNT(_c15) AS trips_per_day\
                    FROM alltrip \
                    GROUP BY medallion, pickup_date \
                    ORDER BY medallion ASC, pickup_date ASC")
car_date_trip.createOrReplaceTempView("date_trip")

car_days_trip = spark.sql("SELECT medallion, COUNT(DISTINCT (pickup_date)) AS days_driven, SUM(trips_per_day) AS total_trips \
                       FROM date_trip\
                       GROUP BY medallion\
                       ORDER BY medallion ASC")
'''
car_days_trip.createOrReplaceTempView("days_trip")

result = spark.sql("SELECT medallion, total_trips, days_driven, (total_trips/days_driven) AS average \
                   FROM days_trip\
                   GROUP BY medallion\
                   ORDER BY medallion ASC")
'''
car_days_trip.select(format_string("%s,%s,%s,%.2f",car_days_trip.medallion, car_days_trip.total_trips, car_days_trip.days_driven, (car_days_trip.total_trips/car_days_trip.days_driven))).write.save("task2d-sql.out",format="text")

'''
#day -- pickup_datetime[3][:10]
# c1 = ((medallion,DATE),1)    (count how many trips per day per car)
# the output of c1 (medallion,date,day_amount)==> (medallion,1,day_amount) (count how many days)
# (medallion,(date,1))
'''