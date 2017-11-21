import os
import sys
import unittest
import os
from pyspark import SparkConf,SparkContext
from pyspark.sql import SQLContext,HiveContext

from pyspark.sql.functions import udf
from pyspark.sql.types import *

import sys

import json


# add pyspark and py4j libs to the PYTHONPATH

from pyspark.tests import ReusedPySparkTestCase

cat = json.dumps({"table":{"namespace":"default", "name":"tbl_users_affected_cnt_hb", "tablecoder":"primitivetype"},"rowkey":"rowkey","columns":{"rowkey":{"cf":"rowkey", "col":"rowkey","type":"string"},\
"accesspointnameni":{"cf":"ericcson1","col":"accesspointnameni","type":"string"},\
"causeforrecclosing":{"cf":"ericcson2","col":"causeforrecclosing","type":"string"},\
"cdr_reason":{"cf":"ericcson3","col":"cdr_reason","type":"string"},\
"duration":{"cf":"ericcson4","col":"duration","type":"string"},\
"recordopeningtime":{"cf":"ericcson5","col":"recordopeningtime","type":"string"},\
"servedimsi":{"cf":"ericcson6","col":"servedimsi","type":"string"},\
"terminated_count":{"cf":"ericcson7","col":"terminated_count","type":"string"}}})

cat_val = json.dumps({"table":{"namespace":"default", "name":"tbl_users_crossed_vollimit_hb", "tablecoder":"primitivetype"},"rowkey":"rowkey","columns":{"rowkey":{"cf":"rowkey", "col":"rowkey","type":"string"},\
"accesspointnameni":{"cf":"ericcson1","col":"accesspointnameni","type":"string"},\
"causeforrecclosing":{"cf":"ericcson2","col":"causeforrecclosing","type":"string"},\
"cdr_reason":{"cf":"ericcson3","col":"cdr_reason","type":"string"},\
"duration":{"cf":"ericcson4","col":"duration","type":"string"},\
"recordopeningtime":{"cf":"ericcson5","col":"recordopeningtime","type":"string"},\
"servedimsi":{"cf":"ericcson6","col":"servedimsi","type":"string"},\
"limit_crossed_cnt":{"cf":"ericcson7","col":"terminated_count","type":"string"}}})


def word_cnt(sqlContext):
    ###sqlContext= SQLContext(sc)
    df=sqlContext.read.option("catalog",cat).option("newtable","2").format("org.apache.spark.sql.execution.datasources.hbase").load()
    df.registerTempTable("tbl_users_affected_cnt_hb")
    a=df.count()
    return a

def word_cnt1(sqlContext):
    ###sqlContext= SQLContext(sc)
    df=sqlContext.read.option("catalog",cat_val).option("newtable","2").format("org.apache.spark.sql.execution.datasources.hbase").load()
    df.registerTempTable("tbl_users_crossed_vollimit_hb")
    a=df.count()
    return a

class SampleTestWithPySparkTestCase(ReusedPySparkTestCase):
    def test_word_cnt(self):
        sqlContext= SQLContext(self.sc)
        self.assertEqual(word_cnt(sqlContext),1,msg='Table tbl_users_affected_cnt_hb does not match the expected result')
    def test_word_cnt1(self):
        sqlContext= SQLContext(self.sc)
        self.assertEqual(word_cnt1(sqlContext),1,msg='Table tbl_users_crossed_vollimit_hb does not match the expected result')

if __name__ == '__main__':
    unittest.main(verbosity = 2)
