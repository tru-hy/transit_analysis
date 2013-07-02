import sys
import csv
import datetime
import sqlite3
import os
from collections import namedtuple
import time

import dateutil
from dateutil import rrule

from transit_analysis import schema
from transit_analysis.utils.hsl import *

def get_departure_record(row):
	route_variant = row.trip_id.split('_')[0]
	route = row.route_id
	shape = row.shape_id
	direction = int(row.direction_id)
	attr = {"gtfs_"+row._fields[i]: str(row[i]) for i in range(len(row))}

	return departure_type(route, route_variant, direction, row.departure_time, shape, attr)

class NamedTupleCsvReader:
	def __init__(self, *args, **kwargs):
		self._reader = iter(csv.reader(*args, **kwargs))
		hdr = self._reader.next()
		self.tupletype = namedtuple('csvtuple', hdr)
	
	def __iter__(self):
		return self
	
	def next(self):
		return self.tupletype(*self._reader.next())

def _parse_date(date):
	return datetime.datetime(int(date[:4]), int(date[4:6]), int(date[6:8]))

def gtfs_departures(path):
	weekday_fields = dict(
		monday=rrule.MO,
		tuesday=rrule.TU,
		wednesday=rrule.WE,
		thursday=rrule.TH,
		friday=rrule.FR,
		saturday=rrule.SA,
		sunday=rrule.SU)
	
	departure_times = {}
	service_dates = {}
	departures = []
	
	def _add_service_dates(row):
		start_date = _parse_date(row.start_date)
		end_date = _parse_date(row.end_date)
		days = []
		for field,daytype in weekday_fields.iteritems():
			if int(getattr(row, field)):
				days.append(daytype)

		dates = list(rrule.rrule(rrule.DAILY, dtstart=start_date, until=end_date,
				byweekday=days))
		
		service_dates[row.service_id] = set()
		for date in dates:
			date = date.strftime("%Y%m%d")
			service_dates[row.service_id].add(date)
			
	
	print >>sys.stderr, "Loading regular services"
	cal_path = os.path.join(path, 'calendar.txt')
	for row in NamedTupleCsvReader(open(cal_path)):
		_add_service_dates(row)

	print >>sys.stderr, "Loading exception dates"
	date_path = os.path.join(path, 'calendar_dates.txt')
	for row in NamedTupleCsvReader(open(date_path)):
		if row.service_id not in service_dates:
			service_dates[row.service_id] = set()
		if row.exception_type == '2':
			try:
				service_dates[row.service_id].remove(row.date)
			except KeyError:
				print >>sys.stderr, "Removal exception of non-existant service"
			continue
		service_dates[row.service_id].add(row.date)
		
	time_path = os.path.join(path, 'stop_times.txt')
	time_reader = NamedTupleCsvReader(open(time_path))
	def get_departure_time(trip_id):
		if trip_id in departure_times:
			return departure_times[trip_id]
		for row in time_reader:
			if row.stop_sequence != '1': continue
			departure_times[row.trip_id] = row.departure_time
			if row.trip_id == trip_id:
				return row.departure_time
		
	print >>sys.stderr, "Loading trips"
	trip_path = os.path.join(path, 'trips.txt')
	trips = NamedTupleCsvReader(open(trip_path))
	departure_tmp_type = namedtuple("departure", list(trips.tupletype._fields)+['departure_time', 'schedule_date'])
		
	for row in trips:
		dates = service_dates[row.service_id]
		time = get_departure_time(row.trip_id)
		h, m, s = map(int, time.split(':'))
		# TODO, FIXME! Breaks on DST-change-days!
		time = datetime.timedelta(hours=h, minutes=m, seconds=s)
		for datestr in dates:
			date = datetime.datetime.strptime(datestr, "%Y%m%d")

			timestamp = date + time
				
			departure = departure_tmp_type(*(list(row) + [timestamp, date]))
			yield departure
		

class Rowtimer:
	def __init__(self):
		self.prev_i = 0
		self.prev_t = time.time()
		self.report_interval = 10000
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

			

def load_hsl_gtfs_departures(gtfsdir, uri=schema.default_uri):
	departures = gtfs_departures(gtfsdir)
	db = schema.connect(uri)
	con = db.bind
	tbl = db.tables['transit_departure']
	timer = Rowtimer()
	batch_size = 3000
	batch = []
	for i, departure in enumerate(departures):
		dep_tuple = get_departure_record(departure)
		values = departure_tuple_to_row(dep_tuple)
		batch.append(values)
		if len(batch) >= batch_size:
			con.execute(tbl.insert(), batch)
			batch = []
		timer(i)

def load_gtfs_shapes(gtfsdir, uri=schema.default_uri):
	shapes = {}
	filepath = os.path.join(gtfsdir, 'shapes.txt')
	# TODO: Could be probably streamed to the database
	for row in NamedTupleCsvReader(open(filepath)):
		if row.shape_id not in shapes:
			shapes[row.shape_id] = []
		shapes[row.shape_id].append((
			int(row.shape_pt_sequence),
			float(row.shape_pt_lat),
			float(row.shape_pt_lon)))
	
	db = schema.connect(uri)
	tbl = db.tables['coordinate_shape']
	for shape_id, coords in shapes.iteritems():
		# Could use a heap if this causes
		# performance problems (probably wont)
		coords.sort()
		latlon = zip(*zip(*coords)[1:])
		tbl.insert(values={
			tbl.c.shape: shape_id,
			tbl.c.coordinates: latlon}).execute()

		
def load_hsl_gtfs(gtfsdir, uri=schema.default_uri):
	load_hsl_gtfs_departures(gtfsdir, uri=uri)
	load_gtfs_shapes(gtfsdir, uri=uri)

if __name__ == '__main__':
	import argh
        parser = argh.ArghParser()
        parser.add_commands([load_hsl_gtfs_departures, load_gtfs_shapes, load_hsl_gtfs])
	parser.dispatch()

