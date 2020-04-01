#!/usr/bin/env python
import sys
from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("task4c-sql").config("spark.some.config.option", "some-value").getOrCreate()
trips_lic = spark.read.format('csv').options(header='false',inferschema='true').load(sys.argv[1])
trips_lic.createOrReplaceTempView("triplic")

#agent_name(20),fare_amount(5)
# (agent_name,fare_amount)  -> (agent_name,total_fare_amount) ->take10

result = spark.sql("SELECT _c20 AS agent_name, SUM(_c5) AS total_revenue\
                     FROM triplic \
                     GROUP BY agent_name\
                     ORDER BY total_revenue DESC, agent_name ASC\
                     LIMIT 10")

result.select(format_string("%s,%.2f",result.agent_name,result.total_revenue)).write.save("task4c-sql.out",format="text")
