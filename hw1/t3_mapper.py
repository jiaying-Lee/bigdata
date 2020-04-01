#!/usr/bin/env python
import os
import re
import string
import sys


# f = open('./hw1data/testdata/licenses.csv', 'r')
# f = open('./hw1data/testdata/licenses_test', 'r')
# f = open('./hw1data/testdata/tripjoinfare.csv', 'r')

# for line in f.readlines():

# # input comes from STDIN (standard input)

for line in sys.stdin:
    if line.find('\t') == -1:
        # if string.find(filename, 'licenses'):
        #     for line in sys.stdin:
        flag = str(2)
        # remove leading and trailing whitespace
        line = line.strip()
        # use re to find driver's name
        name = re.findall(r"\"(.+?)\"", line)
        if name:
            name = '"' + str(name[0]) + '"'
            line = line.replace(name + ',', '')
            features = line.split(',')
            features.insert(1, name)
        else:
            # no name case
            features = line.split(',')

        # split the line into words

        # print len(features)
        if features[0] == 'medallion':
            continue
        print '%s#%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
            features[0], features[1], features[2], features[3], features[4], features[5], features[6], features[7],
            features[8], features[9], features[10], features[11], features[12], features[13], features[14],
            features[15], flag)
    else:
        flag = str(1)
        # remove leading and trailing whitespace
        line = line.strip()
        # split the line into words
        key,features = line.split('\t')

        # split the line into words
        features = features.split(',')
        key = key.split(',')
        print '%s#%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s' % (
        key[0], key[1], key[2], key[3], features[0], features[1], features[2], features[3],
        features[4], features[5], features[6], features[7], features[8], features[9], features[10],
        features[11], features[12], features[13], features[14], features[15], features[16], flag)
