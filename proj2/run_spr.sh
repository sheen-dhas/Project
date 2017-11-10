#!/bin/sh

cp trial.csv /home
chmod 777 trial.csv

apt-get -y update

apt-get install -y openssh-server

spark-submit  --num-executors 5 --driver-memory 2g --executor-memory 2g  --packages com.hortonworks:shc:1.0.0-1.6-s_2.10 --repositories http://repo.hortonworks.com/content/groups/public/ run2.py
