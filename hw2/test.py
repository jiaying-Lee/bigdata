license = 'medallion,name,type,current_status,DMV_license_plate,vehicle_VIN_number,vehicle_type,model_year,medallion_type,agent_number,agent_name,agent_telephone_number,agent_website,agent_address,last_updated_date,last_updated_time'
fares = 'medallion,hack_license,vendor_id,pickup_datetime,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount'
trips = 'medallion,hack_license,vendor_id,rate_code,store_and_fwd_flag,pickup_datetime,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude'
alltrips = 'medallion,hack_license,vendor_id,pickup_datetime,rate_code,store_and_fwd_flag,dropoff_datetime,passenger_count,trip_time_in_secs,trip_distance,pickup_longitude,pickup_latitude,dropoff_longitude,dropoff_latitude,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount'
alltrip_lic='medallion,hack_license,vendor_id,pickup_datetime,payment_type,fare_amount,surcharge,mta_tax,tip_amount,tolls_amount,total_amount,name,type,current_status,DMV_license_plate,vehicle_VIN_number,vehicle_type,model_year,medallion_type,agent_number,agent_name,agent_telephone_number,agent_website,agent_address,last_updated_date,last_updated_time'
features = alltrip_lic.split(',')
print (features[20])
print (features[5])
print (features[8])

# r.select(format_string('%s, %s, %s, %s, %s, %s,\
#             %s, %d, %s, %s, %f, %f,\
#             %f, %f, %s, %f, %f, %f, %f, %f, %f', r.medallion, r.hack_license, r.vendor_id,
#                        date_format(r.pickup_datetime, 'yyyy-MM-dd hh-mm-ss'), r.rate_code, r.store_and_fwd_flag, \
#                        date_format(r.dropoff_datetime, 'yyyy-MM-dd hh-mm-ss'), r.passenger_count, r.trip_time_in_secs,
#                        r.trip_distance, r.pickup_longitude, r.pickup_latitude, \
#                        r.dropoff_longitude, r.dropoff_latitude, r.payment_type, r.fare_amount, r.surcharge, r.mta_tax,
#                        r.tip_amount, r.tolls_amount, r.total_amount \
#                        )).write.save("task1a-sql.out", format="text")
a='aa'
b='bb'
