#!/usr/bin/env python
from operator import itemgetter
import sys

current_key = None
trip = 0
day = 0
current_date = None


# f = open('./hw1data/testdata/t2_e_mapper.output', 'r')
# for line in f.readlines():

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, date = line.split('#')
    # convert count (currently a string) to int
    # try:
    #     revenue = float(revenue)
    #     tolls = float(tolls)
    # except ValueError:
    #     # count was not a number, so silently
    #     # ignore/discard this line
    #     continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_key == key:
        if current_date != date:
            day += 1
            current_date = date
        trip += 1
    else:
        if current_key:
            # write result to STDOUT
            print '%s\t%.2f,%.2f' % (current_key, trip, float(trip)/float(day))
        current_date = date
        current_key = key
        trip = 1
        day = 1

# do not forget to output the last word if needed!
if current_key == key:
    print '%s\t%.2f,%.2f' % (current_key, trip, float(trip)/float(day))