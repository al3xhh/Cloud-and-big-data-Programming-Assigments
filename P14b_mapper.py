#!/usr/bin/python

import sys
import mmap

def findTitle(id):
    with open('../Data/P14_movies_data.csv', 'r') as inF:
        for line in inF:
            if id in line:
                return line.split(",")[1]

for line in sys.stdin:
    split = line.split(";")

    print str(findTitle(split[0])).strip() + ";" + split[1].strip()