#!/bin/sh

ls /*

cd resource-tutorial/proj2

cp trial.csv /home
chmod 777 trial.csv

spark-submit run.py

hbase-shell
