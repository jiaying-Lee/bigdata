#!/usr/bin/env python
import sys


# f = open('./hw1data/testdata/tripjoinfare.csv', 'r')
# for line in f.readlines():
# # input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    key, value = line.split('\t')


    keys = key.split(',')
    medallion = keys[0]
    date = keys[-1][:10]


    print '%s#%s' % (medallion,date)

