#!/usr/bin/python

import sys

count = 0

if len(sys.argv) > 1:
    pattern = sys.argv[1]

    for line in sys.stdin:
        if pattern in line:
            print str(count) + "\t"
        count += 1
else:
    print "You have to introduce the word."