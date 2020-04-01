#!/usr/bin/env python
from operator import itemgetter
import sys

current_key = None
current_revenue = 0.00
current_tolls = 0.00
key = None

# f = open('./hw1data/testdata/t2_d_mapper_output', 'r')
# for line in f.readlines():

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, values = line.split('\t')
    revenue, tolls = values.split(',')
    # convert count (currently a string) to int
    try:
        revenue = float(revenue)
        tolls = float(tolls)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_key == key:
        current_revenue += revenue
        current_tolls += tolls
    else:
        if current_key:
            # write result to STDOUT
            print '%s\t%.2f,%.2f' % (current_key, current_revenue, current_tolls)
        current_revenue = revenue
        current_tolls = tolls
        current_key = key

# do not forget to output the last word if needed!
if current_key == key:
    print '%s\t%.2f,%.2f' % (current_key, current_revenue, current_tolls)