#!/bin/sh

cp trial.csv /home
chmod 777 trial.csv

ls /*

cd /home/build-spark

ls /*

spark-submit run.py

##hbase shell

##cd /opt/phoenix-4.4.0-HBase-1.1-bin/bin

##./sqlline.py localhost:/hbase-unsecure

##ls /*
