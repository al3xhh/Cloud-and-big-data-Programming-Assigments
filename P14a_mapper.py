#!/usr/bin/python

import sys

for line in sys.stdin:
    split = line.split(";")

    print "{};{}".format(int(split[0]), split[1].replace('\n', '').replace('\r', '').replace(' ', ''))