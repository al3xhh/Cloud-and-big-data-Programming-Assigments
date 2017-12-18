#!/usr/bin/python

import sys

for line in sys.stdin:
    url = line.split(" ")[6]

    print url + "\t1"