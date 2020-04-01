#!/usr/bin/env python
from operator import itemgetter
import sys
# the system python does not have numpy, but the python/gnu/2.7.10 does
# (we don't actually need it, but attempting to import it will trigger
# an error if the mapper can't see the version of python we want to use)
#import numpy

current_key = None
current_count = 0
key = None

# f = open('./hw1data/testdata/t2_mapper_out.txt', 'r')
# for line in f.readlines():

# # input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, count = line.split('\t')

    # convert count (currently a string) to int
    try:
        count = int(count)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_key == key:
        current_count += count
    else:
        if current_key:
            # write result to STDOUT
            print '%s\t%s' % (current_key, current_count)
        current_count = count
        current_key = key

# do not forget to output the last word if needed!
if current_key == key:
    print '%s\t%s' % (current_key, current_count)