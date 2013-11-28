#!/usr/bin/env python2
import sys
import bz2
import datetime
import os

SECONDS_IN_HOUR = 60*60

def safeopen(opener, name):
	suffix = 0
	while True:
		if suffix:
			newname = "%s.%i"%(name, suffix)
		else:
			newname = name

		# Overwrite may happen due to a race condition,
		# but it can't be helped with the open-API.
		if not os.path.exists(newname):
			return opener(newname, 'w')

		suffix += 1

def logsplitter(target_format,
		lines_per_file=SECONDS_IN_HOUR,
		input=sys.stdin,
		opener=bz2.BZ2File):
	
	line = input.readline()
	outfile = None
	lines_left = 0
	nth_file = 0
	while line != "":
		# All this just in lack of do-while
		if lines_left == 0:
			if outfile:
				outfile.close()
				outfile = None
				sys.stdout.write(outfilename)
				sys.stdout.write('\n')
				sys.stdout.flush()

			datestr = datetime.datetime.today().isoformat()
			args = dict(timestamp=datestr, nth_file=nth_file)
			outfilename = target_format%args
			outfile = safeopen(opener, outfilename)
			nth_file += 1
			lines_left = lines_per_file
		
		outfile.write(line)
		lines_left -= 1
		line = input.readline()
	
	if outfile:
		outfile.close()
		sys.stdout.write(outfilename)
		sys.stdout.write('\n')
			
if __name__ == '__main__':
	import argh
	argh.dispatch_command(logsplitter)
	
