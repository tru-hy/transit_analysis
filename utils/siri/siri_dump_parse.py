# TODO: Currently expects XML records separated by newlines.
#	The records can't contain newlines in themselves.
#	Very ugly, and unconformant, but, you know, XML.

import sys
import re
import time

from lxml import etree
from transit_analysis.utils.dumping import csvmapper, TraceTracker
from transit_analysis.recordtypes import *

ns_stripper = re.compile(r'xmlns.*?=["\'].*?["\']')

def activity_to_record(act):
	def getfield(name):
		return act.iter(name).next().text
	route = getfield("LineRef")
	direction = getfield("DirectionRef")
	date = getfield("DataFrameRef")
	departure_time = getfield("DatedVehicleJourneyRef")
	timestamp = getfield("RecordedAtTime")
	latitude = float(getfield("Latitude"))
	longitude = float(getfield("Longitude"))
	source = getfield("VehicleRef")

def iterate_activities(infile):
	for raw in infile:
		if raw.strip() == "": continue
		# Nobody uses this crap
		raw = ns_stripper.sub('', raw)
		root = etree.fromstring(raw)
		for act in root.iter("VehicleActivity"):
			yield act

def to_csv(adapter, trace_output="", timezone=""):
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
			trace_output.write(record_str)
			trace_output.write('\n')
			trace_output.flush()
	
		traces = TraceTracker(on_trace)
	else:
		traces = lambda *x: None

	for act in iterate_activities(sys.stdin):
		departure, measurement = handler(act)
		traces(departure, measurement)
		print "\t".join(map(csvmapper, handler(act)[1]))
	traces.finalize()

if __name__ == '__main__':
	import imp
	import argh
	parser = argh.ArghParser()
	parser.add_commands([to_csv])
	parser.dispatch()
	#iterate_records(sys.stdin)
