#!/usr/bin/python

import sys

firstLine = True

for line in sys.stdin:
    if not firstLine:
        url = line.split(" ")[6]

        print url + "\t1"
    else:
        firstLine = False