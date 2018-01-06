#!/usr/bin/python

import sys
from pyspark import SparkContext

sc = SparkContext()
userRatingsRDD = sc.textFile("../../Data/P14_ratings_data.csv")
userRatingsRDD = userRatingsRDD.map(lambda rating: rating.split(","))
userRatingsRDD = userRatingsRDD.map(lambda userRating: (userRating[0], float(userRating[2])))
userRatingsRDD = userRatingsRDD.groupByKey()
userRatingsRDD = userRatingsRDD.map(lambda userRating: (userRating[0], sum(list(userRating[1])) / len(userRating[1])))
userRatingsRDD = userRatingsRDD.sortByKey()
print "Average given by user"
print userRatingsRDD.take(10)