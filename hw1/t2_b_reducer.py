#!/usr/bin/env python
from operator import itemgetter
import sys
# the system python does not have numpy, but the python/gnu/2.7.10 does
# (we don't actually need it, but attempting to import it will trigger
# an error if the mapper can't see the version of python we want to use)


count = 0
key = None

# f = open('./hw1data/testdata/t2_b_mapper_output', 'r')
# for line in f.readlines():

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    key, value = line.split('\t')

    if key == '0,10':
        count += 1

print count