#!/usr/bin/python

import sys
from pyspark import SparkContext

if len(sys.argv) > 1:
    sc = SparkContext()
    meteoriteRDD = sc.textFile("../Data/P3_data.csv")
    meteoriteType = sys.argv[1]
    meteoriteRDD = meteoriteRDD.map(lambda meteorite: meteorite.split(","))
    meteoriteRDD = meteoriteRDD.filter(lambda meteorite: meteoriteType == meteorite[3])
    meteoriteRDD = meteoriteRDD.map(lambda meteorite: (meteorite[6].split(" ")[0].split("/"), meteorite[4].replace(".", "")))
    meteoriteRDD = meteoriteRDD.filter(lambda meteorite: len(meteorite[0]) == 3 and meteorite[1] != "")
    meteoriteRDD = meteoriteRDD.map(lambda meteorite: (meteorite[0][2], int(meteorite[1])))
    meteoriteRDD = meteoriteRDD.groupByKey()
    meteoriteRDD = meteoriteRDD.map(lambda meteorite: (meteorite[0], sum(list(meteorite[1])) / len(meteorite[1])))
    meteoriteRDD = meteoriteRDD.sortByKey()
    print meteoriteRDD.collect()
else:
    print "You have to introduce the meterite class."