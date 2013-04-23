#!/usr/bin/python

import sys
import threading
import linecache
from optparse import OptionParser

# A thread to add up the values of all the lines starting at sline
# and ending at sline+noline, and keep track of the number of lines seen.
class AverageThread(threading.Thread):

  def __init__(self, sline, noline):
    super(threading.Thread, self).__init__(self)
    self.sline = sline # line to start on
    self.noline = noline # number of lines responsible for

  def run(self): 
    global colno

    running_total = 0 # running total for this thread

    for i in xrange(sline, sline+noline):
      # get line from middle of file
      line = linecache.getline(sys.stdin, i)

      # get integer from line
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
          sys.stderr.write("Value in column "+colno+", line "+i+
              " was not integer.\n") 
      else:
        curval = int(tok[colno-1])

      # update running values
      running_total += curval 

    # thread is done, put results of thread additions together
    total_lock.acquire()
    lines += lines_processed
    total += nolines

parser = OptionParser(usage="%prog -c COLUMNNO [options]")
parser.add_option("-c", "--column", action="store", type="int",
    dest="column", default=1, 
    help="specify a column of the .csv to operate on", metavar="COLUMNO")
parser.add_option("-s", "--seperator", action="store", type="string",
    dest="seperator", default=",",
    help="specify a seperator of the csv", metavar="SEPERATOR")
parser.add_option("-t", "--thread", action="store", type="int",
    dest="thread", default=1,
    help="specify how many threads to use", metavar="THREAD")

(options, args) = parser.parse_args()
colno = options.column
sep = options.seperator
lines = 0 # stores number of iterations, and therfore lines in file
total = 0 # total additive value of lines
total_lock = threading.Lock()
