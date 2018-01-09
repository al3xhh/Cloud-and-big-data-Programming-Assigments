#!/usr/bin/python

import sys

firstLine = True

for line in sys.stdin:
    if not firstLine:
        split = line.split(",")
        start = float(split[1])
        end = float(split[2])
        diff = end - start

        print split[0].split("-")[0] + "\t" + str(diff)
    else:
        firstLine = False
