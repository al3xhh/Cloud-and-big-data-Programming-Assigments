#!/usr/bin/python

import sys
from pyspark import SparkContext

sc = SparkContext()
textRDD = sc.textFile('../Data/P11_data.txt')
textRDD = textRDD.map(lambda word: word.replace(',',' ').replace('.',' '). lower())
textRDD = textRDD.flatMap(lambda word: word.split())
textRDD = textRDD.map(lambda word: (word, 1))
textRDD = textRDD.reduceByKey(lambda key, value: key + value)
firstTen = textRDD.take(10)
print firstTen