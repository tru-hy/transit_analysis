import sys
import time
import datetime
from collections import namedtuple

from transit_analysis import schema
from transit_analysis.utils.hsl import *

live_dtype = [
	('id',		str),
	('name',	str),
	('type',	int),
	('ip',		str),
	('lat',		float),
	('lng',		float),
	('speed',	float),
	('bearing',	float),
	('accel',	float),
	('gpsTDelta',	float),
	('gps_ts',	int),
	('low_floor',	str),
	('route',	str),
	('direction',	int),
	('departure',	str),
	('departTime',	lambda x: x),
	('departStartsIn',	int),
	('distFromStart',	float),
	('snappedLat',	float),
	('snappedLng',	float),
	('snappedBearing', float),
	('nextStopIndex', int),
	('onStop',	bool),
	('differenceFromTimetable',	int)
	]

TS_INDEX=zip(*live_dtype)[0].index('departTime')
weird_time_format = "%d%m%Y%H%M%S"
LiveRecord = namedtuple('LiveRecord', zip(*live_dtype)[0])
def convert_ts(weird):
	# Premature optimization
	parts = map(int, (weird[4:8], weird[2:4], weird[0:2], weird[8:10],
			weird[10:12], weird[12:14]))
	return datetime.datetime(*parts)

def key_sanity_check(row):
	if row.gps_ts < 0:
		raise ValueError("Negative timestamp")

def coord_sanity_check(row):
	if row.lat == 0:
		raise ValueError("Zero coordinate")
	

def decode_hsl_row(line):
	fields = line.strip().split(';')
	if len(fields) != len(live_dtype):
		raise ValueError("Wrong number of fields")
	
	fields[TS_INDEX] = convert_ts(fields[TS_INDEX])
	fields = [live_dtype[i][1](fields[i]) for i in xrange(len(fields))]
	row = LiveRecord(*fields)
	key_sanity_check(row)
	return row

class Rowtimer:
	def __init__(self):
		self.prev_i = 0
		self.prev_t = time.time()
		self.report_interval = 100000
		self.next_report = 0
	
	def __call__(self, i):
		if i < self.next_report:
			return
		
		self.next_report += self.report_interval
		t = time.time()
		dt = t - self.prev_t
		n = i - self.prev_i

		print >>sys.stderr, i/1e6, 'M', n/dt, "/s"

		self.prev_t = t
		self.prev_i = i

class DepartureHandler:
	def __init__(self, db):
		self.db = db
		self.departure_tbl = self.db.tables['transit_departure']
		self.trace_tbl = self.db.tables['coordinate_trace']
		self.departure_cache = {}
	
	def __call__(self, row):
		source = row.id
		time = row.departTime
		route_variant = row.route
		direction = row.direction - 1
		key = departure_key(route_variant, direction, time)
		if source in self.departure_cache:
			record = self.departure_cache[source]
			if record[0] == key:
				record[2] = row.gps_ts
				record[3] += 1
				return
			
			self._insert_departure(row, record, source)
		
		self.departure_cache[source] = [key, row.gps_ts, row.gps_ts, 0]
		#print >>sys.stderr, "Started", key
	
	def _insert_departure(self, row, record, source):
		# Skip "out of service" measurements
		if row.route == '': return

		key = record[0]
		# TODO: Upsert
		old = self.departure_tbl.select().\
			where(self.departure_tbl.c.departure_id == key)\
			.execute()
		old = old.fetchone()
		if old is None:
			print >>sys.stderr, "No departure info!", key
			return
		
		if old['trace'] is not None:
			print >>sys.stderr, "Duplicate trace, updating", key

		start_time = datetime.datetime.utcfromtimestamp(record[1]/1000.0)
		end_time = datetime.datetime.utcfromtimestamp(record[2]/1000.0)
		result = self.trace_tbl.insert(dict(
			source=source,
			start_time=start_time,
			end_time=end_time)).execute()
		self.departure_tbl.update().\
			where(self.departure_tbl.c.departure_id == key).\
			values(trace=result.inserted_primary_key[0]).execute()


def load_measurements(skip_traces=False, uri=schema.default_uri):
	if skip_traces:
		trace_listener = lambda x: None
	else:
		trace_listener = DepartureHandler(schema.connect(uri))

	timer = Rowtimer()
	for i, line in enumerate(sys.stdin):
		try:
			row = decode_hsl_row(line)
		except ValueError:
			continue
		trace_listener(row)
		try:
			coord_sanity_check(row)
		except ValueError:
			continue
		rowtime = datetime.datetime.utcfromtimestamp(row.gps_ts/1000.0)
		
		fields = [
			row.id,
			rowtime.isoformat(),
			repr(row.lat),
			repr(row.lng),
			repr(row.bearing),
			'\\N',
			repr(row.speed)
			]
		row = "\t".join(fields)
		if "\n" in row:
			print >>sys.stderr, "Skipping a row with newline due to a hack"
			print repr(row)
		print row
		timer(i)

if __name__ == '__main__':
	import argh
        parser = argh.ArghParser()
        parser.add_commands([load_measurements])
	parser.dispatch()
