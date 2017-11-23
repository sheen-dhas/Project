#!/bin/sh

cp trial.csv /home
chmod 777 trial.csv

cp hosts /home
chmod 777 hosts

apt-get -y update

apt-get install -y openssh-server

/opt/hbase/bin/start-hbase.sh

/opt/hbase/bin/hbase-daemons.sh start zookeeper

/opt/hbase-server

##rm /etc/hosts
##cp /home/hosts /etc/hosts

##hbase shell ./sample_commands.txt

##spark-submit  --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories http://repo.hortonworks.com/content/groups/public/ run.py

##hbase shell ./list.txt

##/usr/hadoop-2.7.3/sbin/stop-all.sh

##rm /usr/hadoop-2.7.3/etc/hadoop/core-site.xml

##cp /home/core-site.xml /usr/hadoop-2.7.3/etc/hadoop

##hadoop namenode -format

##/usr/hadoop-2.7.3/sbin/start-all.sh

hadoop fs -mkdir /tmp/tim_hfile

hadoop fs -mkdir /tmp/tim_hfile/ericcson

hadoop fs -copyFromLocal /tmp/build/df6ad190/resource-tutorial/proj2/646f8e796f044f1d8e459f2d28f4caab /tmp/tim_hfile/ericcson/

hadoop fs -chmod -R 0777 /tmp/tim_hfile/ericcson/*

HADOOP_CLASSPATH=`/opt/hbase/bin/hbase classpath` /usr/hadoop-2.7.3/bin/hadoop jar /opt/hbase/lib/hbase-server-1.2.4.jar completebulkload /tmp/tim_hfile/  tim_ericcson_bulk

hbase shell ./list.txt
