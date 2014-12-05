#!/usr/bin/python

import os
import sys

dir = sys.argv[1]

linecount = 0
for i in os.listdir(dir):
  if i.startswith('part'):
    file = os.path.join(dir, i)
    fin = open(file, 'r')
    for line in fin:
      data = line.split('\t')
      linecount += 1;
      if (data[0] == 'once in a lifetime'):
          print line
print 'Total lines:', linecount

