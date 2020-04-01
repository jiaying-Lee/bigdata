#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task1a-sql").config("spark.some.config.option", "some-value").getOrCreate()
# read first file that first in the commend line --trips
Trips = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
Fares = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])
Trips.createOrReplaceTempView("trip")
Fares.createOrReplaceTempView("fare")

# r = spark.sql("SELECT T.medallion, T.hack_license, T.vendor_id, T.pickup_datetime, T.rate_code, T.store_and_fwd_flag,\
#             T.dropoff_datetime, T.passenger_count, T.trip_time_in_secs, T.trip_distance, T.pickup_longitude, T.pickup_latitude,\
#             T.dropoff_longitude, T.dropoff_latitude, F.payment_type, F.fare_amount, F.surcharge, F.mta_tax, F.tip_amount, F.tolls_amount, F.total_amount\
#             FROM trip T JOIN fare F \
#             ON (T.medallion = F.medallion AND T.hack_license = F.hack_license AND T.vendor_id = F.vendor_id AND T.pickup_datetime = F.pickup_datetime)\
#             ORDER BY T.medallion, T.hack_license, T.pickup_datetime")


result = spark.sql("SELECT t.medallion, t.hack_license, t.vendor_id, t.pickup_datetime, t.rate_code, t.store_and_fwd_flag, \
                   t.dropoff_datetime, t.passenger_count, t.trip_time_in_secs, t.trip_distance, t.pickup_longitude, t.pickup_latitude,\
                    t.dropoff_longitude, t.dropoff_latitude, f.payment_type, f.fare_amount, f.surcharge, f.mta_tax, f.tip_amount, f.tolls_amount, f.total_amount \
            FROM trip t JOIN fare f on t.medallion = f.medallion and t.hack_license = f.hack_license and t.vendor_id = f.vendor_id and t.pickup_datetime = f.pickup_datetime\
             order by t.medallion asc, t.hack_license asc, t.pickup_datetime asc")


'''
r.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%f,%f,%f,%f,%s,%f,%f,%f,%f,%f,%f', r.medallion, r.hack_license, r.vendor_id,
                       date_format(r.pickup_datetime,'yyyy-MM-dd hh:MM:ss'), r.rate_code, r.store_and_fwd_flag,
                       date_format(r.dropoff_datetime, 'yyyy-MM-dd hh:MM:ss'), r.passenger_count, r.trip_time_in_secs,
                       r.trip_distance, r.pickup_longitude, r.pickup_latitude,
                       r.dropoff_longitude, r.dropoff_latitude, r.payment_type, r.fare_amount, r.surcharge, r.mta_tax,
                       r.tip_amount, r.tolls_amount, r.total_amount
                       )).write.save("task1a-sql.out", format="text")
'''
# r.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s', r.medallion, r.hack_license, r.vendor_id,
#                        date_format(r.pickup_datetime,'yyyy-MM-dd hh:MM:ss'), r.rate_code, r.store_and_fwd_flag,
#                        date_format(r.dropoff_datetime, 'yyyy-MM-dd hh:MM:ss'), r.passenger_count, r.trip_time_in_secs,
#                        r.trip_distance, r.pickup_longitude, r.pickup_latitude,
#                        r.dropoff_longitude, r.dropoff_latitude, r.payment_type, r.fare_amount, r.surcharge, r.mta_tax,
#                        r.tip_amount, r.tolls_amount, r.total_amount
#                        )).write.save("task1a-sql.out", format="text")

result.select(format_string('%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%f,%f,%f,%f,%s,%f,%f,%f,%f,%f,%f', result.medallion, result.hack_license, result.vendor_id,
                            date_format(result.pickup_datetime,'yyyy-MM-dd HH:mm:ss'), result.rate_code, result.store_and_fwd_flag, 
                            date_format(result.dropoff_datetime,'yyyy-MM-dd HH:mm:ss'), result.passenger_count, result.trip_time_in_secs,
                            result.trip_distance, result.pickup_longitude, result.pickup_latitude, 
                            result.dropoff_longitude, result.dropoff_latitude, result.payment_type, result.fare_amount, result.surcharge, result.mta_tax,
                            result.tip_amount, result.tolls_amount, result.total_amount)).write.save("task1a-sql.out",format="text")


# trips
# medallion,hack_license,vendor_id,rate_code,store_and_fwd_flag,pickup_datetime,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude

# fares
# medallion,hack_license,vendor_id,pickup_datetime,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount
# F.payment_type, F.fare_amount, F.surcharge, F.mta_tax, F.tip_amount, F.tolls_amount, F.total_amount

# alltrip(21)
# medallion,hack_license,vendor_id,pickup_datetime,rate_code,store_and_fwd_flag,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,
# payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount

# '%s, %s, %s, %s, %s, %s,\
#             %s, %d, %s, %s, %f, %f,\
#             %f, %f, %s, %f, %f, %f, %f, %f, %f'


# r.medallion, r.hack_license, r.vendor_id, date_format(r.pickup_datetime,'yyyy-MM-dd hh-mm-ss'), r.rate_code, r.store_and_fwd_flag, \
#             date_format(r.dropoff_datetime, 'yyyy-MM-dd hh-mm-ss'), r.passenger_count, r.trip_time_in_secs, r.trip_distance, r.pickup_longitude, r.pickup_latitude,\
#             r.dropoff_longitude, r.dropoff_latitude, r.payment_type, r.fare_amount, r.surcharge, r.mta_tax, r.tip_amount, r.tolls_amount, r.total_amount
