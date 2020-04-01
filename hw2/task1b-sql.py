#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task1b-sql").config("spark.some.config.option", "some-value").getOrCreate()
# read first file that first in the commend line --trips
Fares = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
Licenses = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])
Fares.createOrReplaceTempView("fare")
Licenses.createOrReplaceTempView("license")

r = spark.sql("SELECT F.medallion, F.hack_license, F.vendor_id, F.pickup_datetime, F.payment_type,F.fare_amount,F.surcharge,\
            F.mta_tax,F.tip_amount,F.tolls_amount,F.total_amount, L.name,L.type,L.current_status,L.DMV_license_plate,\
            L.vehicle_VIN_number,L.vehicle_type,L.model_year,L.medallion_type,L.agent_number,L.agent_name,\
            L.agent_telephone_number,L.agent_website,L.agent_address,L.last_updated_date,L.last_updated_time \
            FROM fare F join license L \
            ON F.medallion = L.medallion \
            ORDER BY F.medallion ASC, F.hack_license ASC, F.pickup_datetime ASC\
            ")

r.select(format_string('%s,%s,%s,%s,%s,%f,%f,%f,%f,%f,%f,\"%s\",%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s',r.medallion,r.hack_license,r.vendor_id,
                       date_format(r.pickup_datetime, 'yyyy-MM-dd HH:mm:ss'),r.payment_type,r.fare_amount,
                        r.surcharge,r.mta_tax,r.tip_amount,r.tolls_amount,r.total_amount,r.name,r.type,r.current_status,
                        r.DMV_license_plate,r.vehicle_VIN_number,r.vehicle_type,r.model_year,r.medallion_type,r.agent_number,
                        r.agent_name,r.agent_telephone_number,r.agent_website,r.agent_address,r.last_updated_date,
                       date_format(r.last_updated_time, 'HH:mm'))).write.save("task1b-sql.out", format="text")


# fares
# medallion,hack_license,vendor_id,pickup_datetime,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount
# F.payment_type, F.fare_amount, F.surcharge, F.mta_tax, F.tip_amount, F.tolls_amount, F.total_amount

#license
#medallion,name,type,current_status,DMV_license_plate,vehicle_VIN_number,vehicle_type,model_year,medallion_type,agent_number,agent_name,agent_telephone_number,agent_website,agent_address,last_updated_date,last_updated_time

#allfare_lic(26)

# %s,%s,%s,%s,%s,%f,
# %s,%s,%f,%f,%f,%s,%s,%s
# %s,%s,%s,%s,%s,%s,
# %s,%s,%s,%s,%s,%s
#
# r.medallion,r.hack_license,r.vendor_id,date_format(r.pickup_datetime, 'yyyy-MM-dd hh:MM:ss'),r.payment_type,r.fare_amount,\
# r.surcharge,r.mta_tax,r.tip_amount,r.tolls_amount,r.total_amount,r.name,r.type,r.current_status,\
# r.DMV_license_plate,r.vehicle_VIN_number,r.vehicle_type,r.model_year,r.medallion_type,r.agent_number,\
# r.agent_name,r.agent_telephone_number,r.agent_website,r.agent_address,r.last_updated_date,date_format(r.last_updated_time, 'MM/dd/YYYY,hh:MM')
#
# 1/31/2015,13:20