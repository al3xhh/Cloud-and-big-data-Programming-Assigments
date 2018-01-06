#!/usr/bin/python

import sys
from pyspark import SparkContext

sc = SparkContext()
genresRatingsRDD = sc.textFile("../../Data/P14_ratings_data.csv")
genresMoviesRDD = sc.textFile("../../Data/P14_movies_data.csv")
genresRatingsRDD = genresRatingsRDD.map(lambda rating: rating.split(","))
genresMoviesRDD = genresMoviesRDD.map(lambda genre: genre.rsplit(",", genre.count(",")))
genresRatingsRDD = genresRatingsRDD.map(lambda movieRating: (movieRating[1], float(movieRating[2])))
genresMoviesRDD = genresMoviesRDD.map(lambda movieGenre: (movieGenre[0], movieGenre[2]))
genresMoviesRatingsRDD = genresRatingsRDD.join(genresMoviesRDD)
genresMoviesRatingsRDD = genresMoviesRatingsRDD.map(lambda genreMovie: (genreMovie[1][1].split("|"), [genreMovie[1][0]]))
genresMoviesRatingsRDD = genresMoviesRatingsRDD.map(lambda genreMovie: zip(genreMovie[0], genreMovie[1]))
genresMoviesRatingsRDD = genresMoviesRatingsRDD.map(lambda genreMovie: genreMovie[0])
genresMoviesRatingsRDD = genresMoviesRatingsRDD.groupByKey()
genresMoviesRatingsRDD = genresMoviesRatingsRDD.map(lambda genreRating: (genreRating[0], sum(list(genreRating[1])) / len(genreRating[1])))
print "Average rating by genre"
print genresMoviesRatingsRDD.collect()