#!/usr/bin/env python
import sys

# the system python does not have numpy, but the python/gnu/2.7.10 does
# (we don't actually need it, but attempting to import it will trigger
# an error if the mapper can't see the version of python we want to use)

# f = open('./hw1data/testdata/tripjoinfare.csv', 'r')
# for line in f.readlines():
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    key, value = line.split('\t')
    # increase counters
    features = value.split(',')
    passenger_count = int(features[3])

    print passenger_count,'\t1'

