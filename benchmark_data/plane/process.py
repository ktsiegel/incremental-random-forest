#!/usr/bin/python

import sys
import random

assert(len(sys.argv) == 2, "Wrong number of arguments")
findex = sys.argv[1].find(".")
assert(findex != -1, "Filename formatted incorrectly")

fname = sys.argv[1][:findex]

f = open(sys.argv[1], 'r')
fo = open(fname + "_processed.csv", 'w')

firstline = True
for line in f.readlines():
    if line.find("null") == -1 and line.find(",,") == -1:
        if firstline or random.random() < 0.1:
            fo.write(line)
        if firstline:
            firstline = False

f.close()
fo.close()

