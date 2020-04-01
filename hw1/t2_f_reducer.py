#!/usr/bin/env python
from operator import itemgetter
import sys

current_key = None
num = 0
current_taxi = None


# f = open('./hw1data/testdata/t2_f_mapper.out', 'r')
# for line in f.readlines():

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, taxi = line.split('#')

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_key == key:
        if current_taxi != taxi:
            num += 1
            current_taxi = taxi
    else:
        if current_key:
            # write result to STDOUT
            print '%s\t%d' % (current_key, num)
        current_taxi = taxi
        current_key = key
        num = 1


# do not forget to output the last word if needed!
if current_key == key:
    print '%s\t%d' % (current_key, num)