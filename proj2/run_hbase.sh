#!/bin/sh

hbase shell /home/hduser1/hbase_commands.txt

hadoop fs -copyFromLocal /home/hduser1/646f8e796f044f1d8e459f2d28f4caab /tmp/tim_data/ericcson/

HADOOP_CLASSPATH=`/usr/hdp/2.4.2.0-258/hbase/bin/hbase classpath` /usr/hdp/2.4.2.0-258/hadoop/bin/hadoop jar /usr/hdp/2.4.2.0-258/hbase/lib/hbase-server-1.1.2.2.4.2.0-258.jar completebulkload hdfs://mdw.lss.emc.com:8020/tmp/tim_data/  tim_ericcson_bulk

spark-submit  --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories http://repo.hortonworks.com/content/groups/public/ run.py

hbase shell /home/hduser1/list.txt
