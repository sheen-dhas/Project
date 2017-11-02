#!/bin/sh

cp trial.csv /home
chmod 777 trial.csv

spark-submit run.py
