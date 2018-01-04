#!/usr/bin/python

import sys

for line in sys.stdin:
    split = line.split(",")
    start = float(split[1])
    end = float(split[2])
    diff = end - start

    print split[0].split("-")[0] + "\t" + str(diff)
