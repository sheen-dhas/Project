import os
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,HiveContext

from pyspark.sql.functions import udf
from pyspark.sql.types import *


import json

import sys

conf = (SparkConf()
  .setAppName("data_import")
  .set("spark.shuffle.service.enabled","true"))
  

sc = SparkContext(conf = conf)

sqlContext= HiveContext(sc)

cat_main = json.dumps({"table":{"namespace":"default", "name":"employee", "tablecoder":"primitivetype"},"rowkey":"rowkey","columns":{"rowkey":{"cf":"rowkey", "col":"rowkey","type":"string"},\
"id":{"cf":"ericcson","col":"id","type":"string"}}})

df=sqlContext.read.option("catalog",cat_main).option("newtable","2").format("org.apache.spark.sql.execution.datasources.hbase").load()

df.registerTempTable("tim_ericcson_bulk")

aa=sqlContext.sql("select * from tim_ericcson_bulk")

aa.show()
