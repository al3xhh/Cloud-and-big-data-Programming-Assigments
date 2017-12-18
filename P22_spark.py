#!/usr/bin/python

import sys
from pyspark import SparkContext

sc = SparkContext()
logRDD = sc.textFile('../Data/P12_data')
logRDD = logRDD.map(lambda log: log.split())
logRDD = logRDD.map(lambda log: (log[6], 1))
logRDD = logRDD.reduceByKey(lambda key, value: key + value)
firstTen = logRDD.take(10)
print firstTen
