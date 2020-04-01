#!/usr/bin/env python
import sys


# f = open('./hw1data/testdata/tripjoinfare.csv', 'r')
# for line in f.readlines():
# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    key, value = line.split('\t')

    # get date
    keys = key.split(',')
    date = keys[-1][:10]
    # get amount
    features = value.split(',')
    revenue = float(features[-6]) + float(features[-3]) + float(features[-5])
    tolls_amount = float(features[-2])

    print '%s\t%f,%f' % (date,revenue,tolls_amount)

