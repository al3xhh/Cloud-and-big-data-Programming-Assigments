#!/usr/bin/python

import sys
from pyspark import SparkContext

def rating(avgRating):
    if avgRating <= 1:
        return "1"
    elif avgRating > 1 and avgRating <= 2:
        return "2"
    elif avgRating > 2 and avgRating <= 3:
        return "3"
    elif avgRating > 3 and avgRating <= 4:
        return "4"
    else:
        return "5"

sc = SparkContext()
ratingsRDD = sc.textFile("../Data/P14_ratings_data.csv")
moviesRDD = sc.textFile("../Data/P14_movies_data.csv")
ratingsRDD = ratingsRDD.map(lambda rating: rating.split(","))
ratingsRDD = ratingsRDD.map(lambda rating: (rating[1], float(rating[2])))
moviesRDD = moviesRDD.map(lambda movie: movie.split(","))
moviesRatingsRDD = ratingsRDD.join(moviesRDD)
moviesRatingsRDD = moviesRatingsRDD.map(lambda movieRating: (movieRating[1][1], movieRating[1][0]))
moviesRatingsRDD = moviesRatingsRDD.groupByKey()
moviesRatingsRDD = moviesRatingsRDD.map(lambda movieRating: (movieRating[0], sum(list(movieRating[1])) / len(movieRating[1])))
moviesRatingsRDD = moviesRatingsRDD.map(lambda movieRating: (movieRating[0], rating(movieRating[1])))
firstTen = moviesRatingsRDD.take(10)
print firstTen