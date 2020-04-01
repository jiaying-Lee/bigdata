#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3d-sql").config("spark.some.config.option", "some-value").getOrCreate()
all_trips = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
all_trips.createOrReplaceTempView("alltrip")

result = spark.sql("SELECT _c1 AS hack_license, COUNT(DISTINCT (_c0)) AS num_taxis_used\
                    FROM alltrip \
                    GROUP BY hack_license \
                    ORDER BY hack_license ASC")

#(value)medallion key[0] (key)licence [1]

result.select(format_string("%s,%s",result.hack_license, result.num_taxis_used)).write.save("task3d-sql.out",format="text")
