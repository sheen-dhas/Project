#!/bin/sh

export PYSPARK_PYTHON=/spark-1.6.2-bin-hadoop2.6/python

cd /spark-1.6.2-bin-hadoop2.6/bin

./spark-submit /spark-1.6.2-bin-hadoop2.6/examples/src/main/python/wordcount.py
