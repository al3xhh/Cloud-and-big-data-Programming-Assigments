#!/usr/bin/python

import sys

#He normalizado los datos para que salgan en base a 1
for line in sys.stdin:
    split = line.split(";")
    start = int(split[1]) / 1000000
    end = int(split[2]) / 1000000
    diff = end - start

    print split[0].split("/")[2] + "\t" + str(diff)
