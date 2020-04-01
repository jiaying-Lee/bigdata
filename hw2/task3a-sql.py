#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task3a-sql").config("spark.some.config.option", "some-value").getOrCreate()
all_trips = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
all_trips.createOrReplaceTempView("alltrip")


result = spark.sql("SELECT COUNT(_c15) AS p1 FROM alltrip WHERE _c15 < 0 ")
result.select(format_string('%s',result.p1)).write.save("task3a-sql.out", format="text")
