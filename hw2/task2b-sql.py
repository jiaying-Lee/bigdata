#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task2b-sql").config("spark.some.config.option", "some-value").getOrCreate()
all_trips = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
all_trips.createOrReplaceTempView("alltrip")



result = spark.sql("SELECT _c7 AS num_pass, COUNT(*) AS num_trip FROM alltrip GROUP BY _c7 ORDER BY num_pass ASC")
#spark.sql("SELECT _c7 FROM alltrip").show()
result.select(format_string('%s,%s',result.num_pass,result.num_trip)).write.save("task2b-sql.out", format="text")