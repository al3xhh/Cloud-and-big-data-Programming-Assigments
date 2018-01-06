#!/usr/bin/python

import sys
from pyspark import SparkContext

count = 0

def increment():
    global count
    count = count + 1
    return count

if len(sys.argv) > 1:
    pattern = sys.argv[1]

    sc = SparkContext()
    textRDD = sc.textFile('../Data/P11_data.txt')
    textRDD = textRDD.map(lambda word: word.replace(',',' ').replace('.',' '). lower())
    textRDD = textRDD.flatMap(lambda word: word.split())
    textRDD = textRDD.map(lambda word: (word, increment()))
    textRDD = textRDD.filter(lambda word: word[0] == pattern)
    textRDD = textRDD.map(lambda word: word[1])
    firstTen = textRDD.take(10)
    print firstTen
else:
    print "You have to introduce the word."