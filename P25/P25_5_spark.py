#!/usr/bin/python

import sys
from pyspark import SparkContext
import datetime

sc = SparkContext()
RDD1 = sc.textFile("../../Data/P14_ratings_data.csv")
RDD4 = sc.textFile("../../Data/P14_movies_data.csv")

MONTHS = ['Enero', 'Febrero', 'Marzo', 'Abril', 'Mayo', 'Junio', 
            'Julio', 'Agosto', 'Septiembre', 'Octubre', 'Noviembre', 'Diciembre']

def top10(month):
    top10 = list(month[1])
    ret = ""

    for i in range(0, 10):
        ret += top10[i][1][1] +  ","

    return MONTHS[int(month[0]) - 1], ret

RDD4 = RDD4.map(lambda RDD4: RDD4.split(","))
RDD1 = RDD1.map(lambda RDD1: RDD1.split(","))
RDD2 = RDD1.map(lambda RDD1: (RDD1[1], str(datetime.datetime.fromtimestamp(int(RDD1[3])).month)))
RDD1 = RDD1.map(lambda RDD1: (RDD1[1], float(RDD1[2])))
RDD1 = RDD1.groupByKey()
RDD1 = RDD1.map(lambda RDD1: (RDD1[0], float(sum(list(RDD1[1]))) / len(RDD1[1])))
RDD3 = RDD1.join(RDD2)
RDD5 = RDD3.join(RDD4)
RDD5= RDD5.sortBy(lambda RDD5: (-float(RDD5[1][0][0]), -int(RDD5[1][0][1])))
RDD5 = RDD5.groupBy(lambda RDD5: RDD5[1][0][1])
RDD5 = RDD5.sortBy(lambda RDD5: int(RDD5[0]))
RDD5 = RDD5.map(top10)
print RDD5.take(12)