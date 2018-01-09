#!/usr/bin/python

import sys

firstLine = True

for line in sys.stdin:
    if not firstLine:
        split = line.split(",")

        print "{};{}".format(int(split[1]), float(split[2].replace('\n', '').replace('\r', '').replace(' ', '')))
    else:
        firstLine = False