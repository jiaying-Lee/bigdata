#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task2c-sql").config("spark.some.config.option", "some-value").getOrCreate()
all_trips = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
all_trips.createOrReplaceTempView("alltrip")

result = spark.sql("SELECT date_format(_c3, \'yyyy-MM-dd\') AS pickup_date, SUM(_c15)+SUM(_c18)+SUM(_c16) AS total_revenue, SUM(_c19) AS total_amount\
                    FROM alltrip \
                    GROUP BY pickup_date\
                    ORDER BY pickup_date ASC ")

result.select(format_string("%s,%.2f,%.2f",date_format(result.pickup_date, 'yyyy-MM-dd'), result.total_revenue, result.total_amount)).write.save("task2c-sql.out",format="text")

'''
day -- pickup_datetime[3][:10]
total revenue : the fare amount[-6][15], tips[-3][18], and surcharges[-5][16].
tolls_amount [-2][19]
'''