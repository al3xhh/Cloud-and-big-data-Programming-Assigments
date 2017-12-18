#!/usr/bin/python

import sys

previous = None
sum = 0
count = 0

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

for line in sys.stdin:
    key, value = line.split(';')
    
    print key + '\t' + rating(float(value))