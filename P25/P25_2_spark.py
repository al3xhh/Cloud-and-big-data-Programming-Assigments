#!/usr/bin/python

import sys
from pyspark import SparkContext

sc = SparkContext()
overallRatingsRDD = sc.textFile("../../Data/P14_ratings_data.csv")
overallRatingsRDD = overallRatingsRDD.map(lambda rating: rating.split(","))
overallRatingsRDD = overallRatingsRDD.map(lambda userRating: (float(userRating[2])))
overallRatingsRDD = overallRatingsRDD.sum() / overallRatingsRDD.count()
print "Overall average"
print overallRatingsRDD