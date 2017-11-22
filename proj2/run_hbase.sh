#!/bin/sh


# cp trial.csv /home
# chmod 777 trial.csv


# apt-get -y update

# apt-get install -y openssh-server

# /opt/hbase/bin/start-hbase.sh

# /opt/hbase/bin/hbase-daemons.sh start zookeeper

# /opt/hbase-server


hbase shell ./hbase_commands.txt

hadoop fs -copyFromLocal /home/hduser1/tim_data/hfile_data/96379fe66fb4426cb5791f4d84d33d4d /tmp/tim_data/ericcson/

HADOOP_CLASSPATH=`/usr/hdp/2.4.2.0-258/hbase/bin/hbase classpath` /usr/hdp/2.4.2.0-258/hadoop/bin/hadoop jar /usr/hdp/2.4.2.0-258/hbase/lib/hbase-server-1.1.2.2.4.2.0-258.jar completebulkload hdfs://mdw.lss.emc.com:8020/tmp/tim_data/  tim_ericcson_bulk_2



#spark-submit  --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories http://repo.hortonworks.com/content/groups/public/ run.py

#hbase shell ./list.txt

