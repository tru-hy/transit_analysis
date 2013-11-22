#!/usr/bin/env python2

# TODO: Currently expects XML records separated by newlines.
#	The records can't contain newlines in themselves.
#	Very ugly, and unconformant, but, you know, XML.

import sys
import re
import time
from itertools import chain, imap
from collections import namedtuple
from xml.parsers import expat

from transit_analysis.utils.dumping import csvmapper, TraceTracker
#from transit_analysis.recordtypes import *

ns_stripper = re.compile(r'xmlns.*?=["\'].*?["\']')
doctype_stripper = re.compile(r'<\?.*?\?>')

# TODO: Could be a lot faster!

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
		# TODO: See if there's a way to reset
		# expat without creating a new parser, which
		# seems to be quite slow
		self.reset_parser()
		return act


def iterate_activities(infile):
	parser = ActivityParser()
	for raw in infile:
		if raw.strip() == "": continue
		# Nobody uses this crap
		raw = ns_stripper.sub('', raw)
		#raw = doctype_stripper.sub('', raw, count=1)
		#root = etree.fromstring(raw)
		for act in parser.parse(raw):
			yield act
		#for act in root.iter("VehicleActivity"):
		#	yield act

def pop_unfinished_traces(con):
	q = """
	delete from coordinate_measurement
	where finalized = false
	returning *
	"""

	traces = {}
	for row in con.execute(q):
		traces[(row.departure_id, row.source)] = row
	
	return traces

class TraceLoader:
	def __init__(self, con):
		self.finished_check_interval = 60*10
		self.finished_threshold = 60*60
		self.previous_check = None

		self.con = con
		self.traces = pop_unfinished_traces(con)
		self.current_time = None
		
	
	def __call__(self, departure, measurement):
		source, (ts, lat, lon, bearing) = measurement
		self.current_time = ts
		if source == "": return

		key = departure, source
		if key not in self.traces:
			self.traces[key] = dict(
				departure_id=departure,
				source=source,
				start_time=ts,
				time=[],
				latitude=[],
				longitude=[],
				bearing=[],
				finalized=False)

		trace = self.traces[key]
		trace['time'].append((ts - trace['start_time']).total_seconds())
		trace['latitude'].append(lat)
		trace['longitude'].append(lon)
		trace['bearing'].append(bearing)

		if (self.previous_check is None or
		   (ts - self.previous_check).total_seconds() > self.finished_check_interval):
			self.previous_check = ts
			self._handle_finished()

	def _insert(self, trace):
		d, s = trace['departure_id'], trace['source']
		q = """
		select count(*) from coordinate_measurement
		where departure_id=%s and source=%s"""
		if self.con.execute(q, [[d, s]]).fetchone()[0]:
			print >>sys.stderr, ("Ignoring duplicate trace (%s, %s, %i rows)"%(d, s, len(trace['time']))).encode('utf-8')
			return

		vals = trace.values()
		q = "insert into coordinate_measurement (" + ",".join(trace.keys()) + ")"
		q += " values (" +",".join(['%s']*len(vals)) + ")"
		
		
		self.con.execute(q, [vals])

	def _is_finished(self, trace):
		time_from_start = (self.current_time - trace['start_time']).total_seconds()
		return time_from_start - trace['time'][-1] > self.finished_threshold

	def _handle_finished(self):
		for key in self.traces.keys():
			trace = self.traces[key]
			tdiff = self.current_time - trace['start_time']
			if not self._is_finished(trace):
				continue

			if tdiff.total_seconds() + trace['time'][-1] < 2*60*60:
				continue
			
			trace['finalized'] = True
			self._insert(trace)
			del self.traces[key]
	
				
	def finish(self):
		keys = self.traces.keys()
		for key in keys:
			trace = self.traces[key]
			self._insert(trace)
			del self.traces[key]
		self.traces = {}
		

def load(adapter, timezone=""):
	import imp
	from transit_analysis import schema
	adapter = imp.load_source("adapter", adapter)
	
	kwargs = {}
	if timezone:
		args['timezone'] = timezone
	handler = adapter.SiriDepartureMeasurement(**kwargs)

	con = schema.connect().bind.connect()
	with con.begin() as transaction:
		loader = TraceLoader(con)
	
		for act in iterate_activities(sys.stdin):
			try:
				departure, measurement = handler(act)
			except KeyError:
				continue
			loader(departure, measurement)
		loader.finish()
		transaction.commit()

if __name__ == '__main__':
	import imp
	import argh
	parser = argh.ArghParser()
	parser.add_commands([load])
	parser.dispatch()
