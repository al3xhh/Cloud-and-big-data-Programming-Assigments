#!/usr/bin/python

import sys
from pyspark import SparkContext
import datetime

sc = SparkContext()
RDD = sc.textFile("../Data/P25_ratings_data.csv")
userRatingsRDD = RDD
overallRatingsRDD = RDD
moviesRatingsRDD = RDD
genresRatingsRDD = RDD
timestampRDD = RDD
genresMoviesRDD = sc.textFile("../Data/P25_movies_data.csv")
moviesTitlesRDD = genresMoviesRDD

'''
userRatingsRDD = userRatingsRDD.map(lambda rating: rating.split(";"))
userRatingsRDD = userRatingsRDD.map(lambda userRating: (userRating[0], float(userRating[2])))
userRatingsRDD = userRatingsRDD.groupByKey()
userRatingsRDD = userRatingsRDD.map(lambda userRating: (userRating[0], sum(list(userRating[1])) / len(userRating[1])))
userRatingsRDD = userRatingsRDD.sortByKey()
print "Average given by user"
print userRatingsRDD.take(10)

overallRatingsRDD = overallRatingsRDD.map(lambda rating: rating.split(";"))
overallRatingsRDD = overallRatingsRDD.map(lambda userRating: (float(userRating[2])))
overallRatingsRDD = overallRatingsRDD.sum() / overallRatingsRDD.count()
print "Overall average"
print overallRatingsRDD'''

moviesRatingsRDD = moviesRatingsRDD.map(lambda rating: rating.split(";"))
moviesRatingsRDD = moviesRatingsRDD.map(lambda movieRating: (movieRating[1], float(movieRating[2])))
moviesRatingsRDD = moviesRatingsRDD.groupByKey()
moviesRatingsRDD = moviesRatingsRDD.map(lambda movieRating: (movieRating[0], sum(list(movieRating[1])) / len(movieRating[1])))
moviesRatingsRDD = moviesRatingsRDD.sortByKey()
print "Average rating by movie"
print moviesRatingsRDD.take(10)

'''
def splitGenres(tuple):
    for genre in tuple[0]:
        return genre, tuple[1]

genresRatingsRDD = genresRatingsRDD.map(lambda rating: rating.split(";"))
genresMoviesRDD = genresMoviesRDD.map(lambda genre: genre.split(";"))
genresRatingsRDD = genresRatingsRDD.map(lambda movieRating: (movieRating[1], float(movieRating[2])))
genresMoviesRDD = genresMoviesRDD.map(lambda movieGenre: (movieGenre[0], movieGenre[2]))
genresMoviesRatingsRDD = genresRatingsRDD.join(genresMoviesRDD)
genresMoviesRatingsRDD = genresMoviesRatingsRDD.map(lambda genreMovie: (genreMovie[1][1].split("|"), genreMovie[1][0]))
genresMoviesRatingsRDD = genresMoviesRatingsRDD.map(splitGenres)
genresMoviesRatingsRDD = genresMoviesRatingsRDD.groupByKey()
genresMoviesRatingsRDD = genresMoviesRatingsRDD.map(lambda genreRating: (genreRating[0], sum(list(genreRating[1])) / len(genreRating[1])))
print "Average rating by genre"
print genresMoviesRatingsRDD.take(10)'''

moviesTitlesRDD = moviesTitlesRDD.map(lambda movie: movie.split(";"))
moviesTitlesRDD = moviesTitlesRDD.map(lambda movie: (movie[0], movie[1]))
topMoviesRatingsRDD = moviesRatingsRDD.join(moviesTitlesRDD)
topMoviesRatingsRDD = topMoviesRatingsRDD.map(lambda movieRating: (movieRating[1][0], movieRating[1][1] + "," + movieRating[0]))
topMoviesRatingsRDD = topMoviesRatingsRDD.sortByKey(False)
topMoviesRatingsIdsRDD = topMoviesRatingsRDD
topMoviesRatingsIdsRDD = topMoviesRatingsIdsRDD.map(lambda movieId: (movieId[1].split(",")[1], movieId[1].split(",")[0]))
topMoviesRatingsRDD = topMoviesRatingsRDD.map(lambda movieRating: (movieRating[1], movieRating[0]))
print "Top 10"
print topMoviesRatingsRDD.take(10)

def top10(tuple):
    ret = ""
    tupleList = list(tuple[1])

    for i in range(0, 10):
        ret += tupleList[i] + "\n"

    return str(tuple[0]) + "\n" + ret

timestampRDD = timestampRDD.map(lambda timestamp: timestamp.split(";"))
ratingTimestampRDD = timestampRDD.map(lambda timestamp: (timestamp[1], str(timestamp[2]) + ";" + str(datetime.datetime.fromtimestamp(int(timestamp[3])).month)))
moviesRatingTimestampRDD = ratingTimestampRDD.join(topMoviesRatingsIdsRDD)
moviesRatingTimestampRDD = moviesRatingTimestampRDD.map(lambda movie: (int(movie[1][0].split(";")[1]), movie[1][1]))
moviesRatingTimestampRDD = moviesRatingTimestampRDD.groupByKey()
moviesRatingTimestampRDD = moviesRatingTimestampRDD.sortByKey()
moviesRatingTimestampRDD = moviesRatingTimestampRDD.map(top10)
#movieRatingTimestampRDD = ratingTimestampRDD.join(movieTimestampRDD)
#movieRatingTimestampRDD = movieTimestampRDD.groupByKey()
#movieRatingTimestampRDD = movieRatingTimestampRDD.sortByKey()
print "Top 10 by month"
print moviesRatingTimestampRDD.take(12)