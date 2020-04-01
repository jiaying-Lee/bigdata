#!/usr/bin/env python
import sys

# the system python does not have numpy, but the python/gnu/2.7.10 does
# (we don't actually need it, but attempting to import it will trigger
# an error if the mapper can't see the version of python we want to use)

#
# f = open('./hw1data/testdata/fare_data.csv', 'r')
# f.readline()
# counter = 0
# input comes from STDIN (standard input)
# for line in f.readlines():
for line in sys.stdin:
    #counter += 1
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    features = line.split(',')
    # increase counters
    # for feature in features:
    #     # write the results to STDOUT (standard output);
    #     # what we output here will be the input for the
    #     # Reduce step, i.e. the input for reducer.py
    #     #
    #     # tab-delimited; the trivial word count is 1
    #     medallion =
    #     if w:
    #         print '%s\t%s' % (w.lower(), 1)
    if features[0] == 'medallion':
        continue
    # 2 means fare
    if len(features) == 11:
        flag = str(2)
        # print '%s,%s,%s,%s#%s,%s,%s,%s,%s,%s,%s,%s' % (
        # features[0], features[1], features[2], features[3], features[4], features[5], features[6], features[7],
        # features[8], features[9], features[10], flag)
        print str(features[0]) + ',' + str(features[1]) + ',' + str(features[2]) + ',' + str(features[3]) + '#' + str(
            features[4]) + ',' + str(features[5]) + ',' + str(features[6]) + ',' + str(features[7]) + ',' + str(
            features[8]) + ',' + str(features[9]) + ',' + str(features[10]) + ',' + flag
    # 1 means trip
    if len(features) == 14:
        flag = str(1)
        # if counter > 20:
        # print '%s,%s,%s,%s#%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
        # features[0], features[1], features[2], features[5], features[3], features[4], features[6], features[7],
        # features[8], features[9], features[10], features[11], features[12], features[13], flag)
        print str(features[0]) + ',' + str(features[1]) + ',' + str(features[2]) + ',' + str(features[5]) + '#' + str(
            features[3]) + ',' + str(features[4]) + ',' + str(features[6]) + ',' + str(features[7]) + ',' + str(
            features[8]) + ',' + str(features[9]) + ',' + str(features[10]) + ',' + str(features[11]) + ',' + str(
            features[12]) + ',' + str(features[13]) + ',' + flag
    # if counter == 30:
    #     break
