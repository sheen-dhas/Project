import os
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,HiveContext

import sys

conf = (SparkConf()
  .setAppName("data_import")
  .set("spark.shuffle.service.enabled","true"))

sc = SparkContext(conf = conf)
 
sqlContext= SQLContext(sc)

distData = sc.textFile("file:///resource-tutorial/proj2/trial.csv")
data=distData.map(lambda l:l.split(","))
fm=sqlContext.createDataFrame(data)
fm.show()
