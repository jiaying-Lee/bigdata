#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task4a-sql").config("spark.some.config.option", "some-value").getOrCreate()
trips_lic = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
trips_lic.createOrReplaceTempView("triplic")

#vehicle_type(16),fare_amount(5),tip_amount(8)
# (vehicle_type,(fare_amount,tip_amount/fare_amount,1)  -> (vehicle_type,(total_fare_amount,total_tip_amount/fare_amount,total_trip)
#output (vehicle_type, total_trips, total_revenue, avg_tip_percentage)


total_revenue = spark.sql("SELECT _c16 AS vehicle_type, SUM(_c5) AS revenue, COUNT(*) AS total_trips, SUM(CASE WHEN _c5 = 0 THEN 0 ELSE _c8/_c5 END) AS avg_tip_percentage\
                     FROM triplic \
                     GROUP BY vehicle_type\
                     ORDER BY vehicle_type")

total_revenue.select(format_string("%s,%s,%.2f,%.2f",total_revenue.vehicle_type, total_revenue.total_trips,total_revenue.revenue, (total_revenue.avg_tip_percentage/total_revenue.total_trips)*100)).write.save("task4a-sql.out",format="text")