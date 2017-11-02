import os
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,HiveContext

import sys

conf = (SparkConf()
  .setAppName("data_import")
  .set("spark.shuffle.service.enabled","true"))

sc = SparkContext(conf = conf)
 
sqlContext= SQLContext(sc)

data = [1, 2, 3, 4, 5]
distData = sc.parallelize(data)

distData.collect()
