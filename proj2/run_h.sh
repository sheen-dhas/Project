#!/bin/sh

cp trial.csv /home
chmod 777 trial.csv

apt-get -y update

apt-get install -y openssh-server

/opt/hbase/bin/start-hbase.sh

/opt/hbase/bin/hbase-daemons.sh start zookeeper

/opt/hbase-server


##hbase shell ./sample_commands.txt

##spark-submit  --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories http://repo.hortonworks.com/content/groups/public/ run.py

##hbase shell ./list.txt

hadoop fs -mkdir /tmp/tim_hfile

hadoop fs -mkdir /tmp/tim_hfile/ericcson

hadoop fs -copyFromLocal /tmp/build/df6ad190/resource-tutorial/proj2/646f8e796f044f1d8e459f2d28f4caab /tmp/tim_hfile/ericcson/

hadoop fs -chmod -R 0777 /tmp/tim_hfile/ericcson/*

hadoop fs -ls /tmp/tim_hfile/ericcson/

##hadoop fs -ls /tmp/tim_hfile/ericcson/646f8e796f044f1d8e459f2d28f4caab

HADOOP_CLASSPATH=`/opt/hbase/bin/hbase classpath` /usr/hadoop-2.7.3/bin/hadoop jar /opt/hbase/lib/hbase-server-1.2.4.jar completebulkload hdfs://10.254.0.26:8020/tmp/tim_hfile/  tim_ericcson_bulk
