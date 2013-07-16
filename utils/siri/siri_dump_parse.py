# TODO: Currently expects XML records separated by newlines.
#	The records can't contain newlines in themselves.
#	Very ugly, and unconformant, but, you know, XML.

import sys
import re
import time

try:
	from lxml import etree
except ImportError:
	import xml.etree.ElementTree as etree

from xml.parsers import expat

from transit_analysis.utils.dumping import csvmapper, TraceTracker
from transit_analysis.recordtypes import *

ns_stripper = re.compile(r'xmlns.*?=["\'].*?["\']')

class ActivityParser:
	def __init__(self):
		self.reset_parser()

	def reset_parser(self):
		self.parser = expat.ParserCreate()
		self.parser.StartElementHandler = self.start
		self.parser.EndElementHandler = self.end
		self.parser.CharacterDataHandler = self.data
		self.activities = []
		self.current_field = None
		self.in_activity = False

	def start(self, name, attr):
		if name == 'VehicleActivity':
			self.in_activity = True
			self.activities.append({})
		elif not self.in_activity:
			return
		self.current_field = name
	
	def end(self, name):
		if name == 'VehicleActivity':
			self.in_activity = False
			self.current_field = None
	
	def data(self, data):
		if not self.in_activity: return
		self.activities[-1][self.current_field] = data
	
	def parse(self, data):
		try:
			self.parser.Parse(data)
		except expat.ExpatError:
			pass
		act = self.activities
		self.reset_parser()
		return act
	
	

def iterate_activities(infile):
	parser = ActivityParser()
	for raw in infile:
		if raw.strip() == "": continue
		# Nobody uses this crap
		raw = ns_stripper.sub('', raw)
		#root = etree.fromstring(raw)
		for act in parser.parse(raw):
			yield act
		#for act in root.iter("VehicleActivity"):
		#	yield act

def to_csv(adapter, trace_output="", timezone="", dump_departure=False,
		no_measurements=False):
	import imp
	adapter = imp.load_source("adapter", adapter)
	
	kwargs = {}
	if timezone:
		args['timezone'] = timezone
	handler = adapter.SiriDepartureMeasurement(**kwargs)

	if trace_output:
		trace_output = open(trace_output, 'w')
		def on_trace(src, departure, start, end):
			trace_id = "transit_departure/"+departure
			record = coordinate_trace(
				id=trace_id,
				source=src,
				start_time=start,
				end_time=end)
			record_str = "	".join(map(csvmapper, record))
			trace_output.write(record_str.encode('utf-8'))
			trace_output.write('\n')
			trace_output.flush()
	
		traces = TraceTracker(on_trace)
	else:
		traces = lambda *x: None

	for act in iterate_activities(sys.stdin):
		try:
			departure, measurement = handler(act)
		except KeyError:
			continue
		traces(departure, measurement)
		if no_measurements: continue
		line = "\t".join(map(csvmapper, handler(act)[1]))
		sys.stdout.write(line.encode("utf-8"))
		if dump_departure:
			sys.stdout.write("\t")
			sys.stdout.write(departure.encode("utf-8"))
		sys.stdout.write('\n')
		sys.stdout.flush()
	traces.finalize()

if __name__ == '__main__':
	import imp
	import argh
	parser = argh.ArghParser()
	parser.add_commands([to_csv])
	parser.dispatch()
	#iterate_records(sys.stdin)
