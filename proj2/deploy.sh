#!/bin/sh

hbase shell ./sample_commands.txt

spark-submit  --packages com.hortonworks:shc-core:1.1.1-2.1-s_2.11 --repositories http://repo.hortonworks.com/content/groups/public/ run.py

hbase shell ./list.txt
