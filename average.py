#!/usr/bin/python

import sys
import threading
import linecache
from optparse import OptionParser

# A thread to add up the values of all the lines starting at sline
# and ending at sline+noline, and keep track of the number of lines seen.
class AverageThread(threading.Thread):

  def __init__(self, file_name, thread_id, total_threads):
    threading.Thread.__init__(self)
    self.file_name = file_name
    self.thread_id = thread_id # which number thread this is. starts at 0
    self.total_threads = total_threads # number of total threads that there are.

  def run(self): 
    global colno, total_lock, lines, total, sep

    running_total = 0 # running total for this thread
    lines_processed = 0 # number of lines processed by thread

    # take every thread_idth line
    line = linecache.getline(self.file_name, self.thread_id) # initialize the line variable
    while line != '':

      # get integer from line
      tok = line.split(sep)
      if len(tok) < colno:
        sys.stderr.write("Specified column number does not exist in .csv file.\n")
        sys.exit(1)

      # get integer from line
      curval = 0
      if lines_processed == 0:
        try:
          curval = float(tok[colno-1])
        except exceptions.ValueError:
          sys.stderr.write("Value in column "+colno+", line "+i+
              " was not integer.\n") 
      else:
        curval = int(tok[colno-1])

      # update running values
      running_total += curval 
      lines_processed += 1
      line = linecache.getline(self.file_name, self.thread_id + lines_processed
          * self.total_threads) # initialize the line variable

    # thread is done, put results of thread additions together
    total_lock.acquire()
    lines += lines_processed
    total += running_total
    total_lock.release()

parser = OptionParser(usage="%prog -f FILE -c COLUMNNO [options]")
parser.add_option("-f", "--file", action="store", type="string",
    dest="file", 
    help="specify file name", metavar="FILE")
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
file_name = options.file
colno = options.column
sep = options.seperator
threads = options.thread
lines = 0 # stores number of iterations, and therfore lines in file
total = 0 # total additive value of lines
total_lock = threading.Lock() # a lock on the above two values

# spawn and run threads
thread_pool = []
for i in xrange(threads): 
  cur_thread = AverageThread(file_name, i+1, threads) 
  cur_thread.start()
  thread_pool.append(cur_thread)

# wait for all threads to finish
for t in thread_pool:
  t.join()

total_lock.acquire()
print "total: "+str(total)
print "lines: "+str(lines)
print "average: "+str(float(total) / float(lines))
total_lock.release()
