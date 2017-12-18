#!/usr/bin/python

import sys

previous = None
sum = 0
count = 0

for line in sys.stdin:
    key, value = line.split(';')
    value.replace('\n', '')

    if key != previous:
        if previous is not None:
            print previous + ';' + str(float(sum) / count)

        previous = key
        sum = 0
        count = 0
    
    sum = sum + float( value )
    count += 1

print previous + ';' + str(float(sum) / count)