#!/usr/bin/python

import sys
from optparse import OptionParser

parser = OptionParser(usage="%prog -c COLUMNNO [options]")
parser.add_option("-c", "--column", action="store", type="int",
    dest="column", default=1, 
    help="specify a column of the .csv to operate on", metavar="COLUMNO")
parser.add_option("-s", "--seperator", action="store", type="string",
    dest="seperator", default=",",
    help="specify a seperator of the csv", metavar="SEPERATOR")

(options, args) = parser.parse_args()
colno = options.column
sep = options.seperator
lines = 0 # stores number of iterations, and therfore lines in file
total = 0 # total additive value of lines

for line in sys.stdin:
  lines += 1

  # tokenize line
  tok = line.split(sep)
  if len(tok) < colno:
    sys.stderr.write("Specified column number does not exist in .csv file.\n")
    sys.exit(1)
  
  # get integer from line
  curval = 0
  if lines == 1:
    try:
      curval = float(tok[colno-1])
    except exceptions.ValueError:
      sys.stderr.write("Value in column "+colno+", line "+lines+
          " was not integer.\n") 
  else:
    curval = int(tok[colno-1])

  # add value
  total += curval

# average value is calculated here
average = total / float(lines)

print average
