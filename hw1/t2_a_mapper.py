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
    fare_amount = float(features[-6])

    if 0 <= fare_amount <= 4:
        print '0,4\t1'
    elif 4.01 <= fare_amount <= 8:
        print '4.01,8\t1'
    elif 8.01 <= fare_amount <= 12:
        print '8.01,12\t1'
    elif 12.01 <= fare_amount <= 16:
        print '12.01,16\t1'
    elif 16.01 <= fare_amount <= 20:
        print '16.01,20\t1'
    elif 20.01 <= fare_amount <= 24:
        print '20.01,24\t1'
    elif 24.01 <= fare_amount <= 28:
        print '24.01,28\t1'
    elif 28.01 <= fare_amount <= 32:
        print '28.01,32\t1'
    elif 32.01 <= fare_amount <= 36:
        print '32.01,36\t1'
    elif 36.01 <= fare_amount <= 40:
        print '36.01,40\t1'
    elif 40.01 <= fare_amount <= 44:
        print '40.01,44\t1'
    elif 44.01 <= fare_amount <= 48:
        print '44.01,48\t1'
    elif fare_amount > 48:
        print '48.01,infinite\t1'
    else:
        continue

