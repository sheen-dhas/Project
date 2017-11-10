from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()


import json

import sys

cat_main = json.dumps({"table":{"namespace":"default", "name":"employee", "tablecoder":"primitivetype"},"rowkey":"rowkey","columns":{"rowkey":{"cf":"rowkey", "col":"rowkey","type":"string"},\
"id":{"cf":"ericcson","col":"id","type":"string"}}})

df=spark.read.option("catalog",cat_main).option("newtable","2").format("org.apache.spark.sql.execution.datasources.hbase").load()

df.registerTempTable("tim_ericcson_bulk")

aa=spark.sql("select * from tim_ericcson_bulk")

aa.show()
