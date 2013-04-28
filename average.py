#!/usr/bin/python

import sys
import multiprocessing
import linecache
from optparse import OptionParser
from datetime import datetime

def AverageProcess(queue, proc_id, total_procs, colno, file_name, sep): 

  running_total = 0 # running total for this process
  lines_processed = 0 # number of lines processed by process

  # take every proc_idth line
  line = linecache.getline(file_name, proc_id) # initialize the line variable
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
    line = linecache.getline(file_name, proc_id + lines_processed
        * total_procs) # initialize the line variable

  # process is done, put results of process additions together
  queue.put([running_total, lines_processed], True)
  return

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
parser.add_option("-p", "--processes", action="store", type="int",
    dest="process", default=1,
    help="specify how many procs to use", metavar="PROCESS")

# parse arguments
(options, args) = parser.parse_args()
# create variables
file_name = options.file
colno = options.column
sep = options.seperator
procs = options.process
total = 0
lines = 0
queue = multiprocessing.Queue()

# start timing
start_time = datetime.now()

# spawn and run procs
process_pool = []
for i in xrange(procs): 
  cur_proc = multiprocessing.Process(target=AverageProcess, args=(queue, i+1,
    procs, colno, file_name, sep))
  cur_proc.start()
  process_pool.append(cur_proc)

# wait for all procs to finish, removing and adding a value each time
for p in process_pool:
  p.join()
  item = queue.get() # block until we get an item. 
  total += item[0]
  lines += item[1]

# end timing
total_time = datetime.now()-start_time

print "average: "+str(float(total) / float(lines))
print "microseconds elapsed: "+str(total_time.microseconds)
print "lines: "+str(lines)
print "microseconds per line with "+str(procs)+" procs: "+str(total_time.microseconds / float(lines))
