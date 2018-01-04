#!/usr/bin/python

import sys

for line in sys.stdin:
    split = line.split(",")

    print "{};{}".format(int(split[1]), float(split[2].replace('\n', '').replace('\r', '').replace(' ', '')))