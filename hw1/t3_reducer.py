#!/usr/bin/env python

import sys


# def list_info(key, current_value, value):
#     if current_value[-1] == '1':
#         return '%s\t%s,%s' % (key, current_value[:-2], value[:-2])
#     else:
#         return '%s\t%s,%s' % (key, value[:-2], current_value[:-2])

def join_list(same_key,key):
    l1 = []
    l2 = []
    for value in same_key:
        if value[-1] == '1':
            l1.append(value[:-2])
        else:
            l2.append(value[:-2])
    for v1 in l1:
        for v2 in l2:
            print '%s\t%s,%s' % (key,v1,v2)

current_key = None
key = None
same_key = []

# f = open('./hw1data/testdata/t3_mapper.out', 'r')
# for line in f.readlines():
# #input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    key, value = line.split('#')
    if current_key == key:
        same_key.append(value)
        #print list_info(key,current_value,value)
    else:
        join_list(same_key,current_key)
        same_key = [value]
        current_key = key
        #current_value = value
# do not forget to output the last word if needed!
if current_key == key:
    join_list(same_key, key)