#!/usr/bin/python

import sys
from pyspark import SparkContext

sc = SparkContext()
meteoriteRDD = sc.textFile("../Data/P3_data.csv")
meteoriteType = sys.argv[1]
meteoriteRDD = meteoriteRDD.map(lambda meteorite: meteorite.split(";"))
meteoriteRDD = meteoriteRDD.filter(lambda meteorite: meteoriteType == meteorite[3])
meteoriteRDD = meteoriteRDD.map(lambda meteorite: (meteorite[6].split(" ")[0].split("/"), meteorite[4].replace(".", "")))
print meteoriteRDD.collect()