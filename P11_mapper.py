#!/usr/bin/python

import sys

count = 0
pattern = sys.argv[1]

for line in sys.stdin:
    if pattern in line:
        print str(count) + "\t"
    count += 1