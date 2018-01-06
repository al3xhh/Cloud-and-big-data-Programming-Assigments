#!/usr/bin/python

import sys
from pyspark import SparkContext

sc = SparkContext()
moviesRatingsRDD = sc.textFile("../../Data/P14_ratings_data.csv")
moviesRatingsRDD = moviesRatingsRDD.map(lambda rating: rating.split(","))
moviesRatingsRDD = moviesRatingsRDD.map(lambda movieRating: (movieRating[1], float(movieRating[2])))
moviesRatingsRDD = moviesRatingsRDD.groupByKey()
moviesRatingsRDD = moviesRatingsRDD.map(lambda movieRating: (movieRating[0], sum(list(movieRating[1])) / len(movieRating[1])))
moviesRatingsRDD = moviesRatingsRDD.sortByKey()
print "Average rating by movie"
print moviesRatingsRDD.take(10)