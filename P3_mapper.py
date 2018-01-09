#!/usr/bin/python

import sys

meteoriteType = sys.argv[1]
firstLine = True

if len(sys.argv) > 1:
    for line in sys.stdin:
        if not firstLine:
            meteoriteClass = line.split(",")[3]

            if meteoriteType ==  meteoriteClass and line.split(",")[4] != "":
                meteoriteMass = line.split(",")[4].replace(".", "")

                print line.split(",")[6].split(" ")[0].split("/")[2] + "\t" + meteoriteMass
        else:
            firstLine = False
else:
    print "You have to introduce the meterite class."