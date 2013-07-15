from collections import namedtuple
import os
import csv
import codecs
import time
import datetime
import sys
from itertools import chain

import dateutil
from dateutil import rrule
import pytz


def bomopen(path):
	f = open(path)
	c = f.read(3)
	if c != codecs.BOM_UTF8:
		f.seek(0)
	return f
	
def _parse_date(date):
	return datetime.datetime(int(date[:4]), int(date[4:6]), int(date[6:8]))

class NamedTupleCsvReader:
	def __init__(self, *args, **kwargs):
		self._reader = iter(csv.reader(*args, **kwargs))
		hdr = self._reader.next()
		self.tupletype = namedtuple('csvtuple', hdr)
	
	def __iter__(self):
		return self
	
	def next(self):
		return self.tupletype(*self._reader.next())


class StopTimes(object):
	def __init__(self, directory):
		self.reader = NamedTupleCsvReader(
			bomopen(os.path.join(directory, 'stop_times.txt'))
			)
		self.leftover = []
		self.per_trip = {}
	
	def _read_next_trip(self):
		prev_id = None
		rows = []
		rowiter = chain(self.leftover, self.reader)
		self.leftover = []
		for row in rowiter:
			if prev_id is not None and row.trip_id != prev_id:
				self.leftover = [row]
				break
			prev_id = row.trip_id
			rows.append(row)

		if prev_id is None:
			raise KeyError
		return prev_id, rows
	
	def __getitem__(self, tripid):
		if tripid in self.per_trip:
			return self.per_trip[tripid]
		
		previd = None
		while True:
			newid, data = self._read_next_trip()
			self.per_trip[tripid] = data
			if newid == tripid:
				return data
		
		raise KeyError

def gtfs_departures(path, stop_times, timezone):
	# TODO! Fix timezoning! (Seems to imply GMT at the moment)
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
	tz = pytz.timezone(timezone)
	
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
	for row in NamedTupleCsvReader(bomopen(cal_path)):
		_add_service_dates(row)

	print >>sys.stderr, "Loading exception dates"
	date_path = os.path.join(path, 'calendar_dates.txt')
	for row in NamedTupleCsvReader(bomopen(date_path)):
		if row.service_id not in service_dates:
			service_dates[row.service_id] = set()
		if row.exception_type == '2':
			try:
				service_dates[row.service_id].remove(row.date)
			except KeyError:
				print >>sys.stderr, "Removal exception of non-existant service"
			continue
		service_dates[row.service_id].add(row.date)
		
	print >>sys.stderr, "Loading trips"
	trip_path = os.path.join(path, 'trips.txt')
	trips = NamedTupleCsvReader(bomopen(trip_path))
	departure_tmp_type = namedtuple("departure", list(trips.tupletype._fields)+['departure_time', 'schedule_date'])
		
	for row in trips:
		dates = service_dates[row.service_id]
		#time = get_departure_time(row.trip_id)
		time = stop_times[row.trip_id][0].departure_time
		assert stop_times[row.trip_id][0].stop_sequence == '1'
		h, m, s = map(int, time.split(':'))
		# TODO, FIXME! Breaks on DST-change-days!
		# TODO, FIXME! Timezone!
		time = datetime.timedelta(hours=h, minutes=m, seconds=s)
		for datestr in dates:
			date = datetime.datetime.strptime(datestr, "%Y%m%d")

			timestamp = tz.normalize(tz.localize(date + time)).astimezone(pytz.utc)
			timestamp = timestamp.replace(tzinfo=None)
			
			departure = departure_tmp_type(*(list(row) + [timestamp, date]))
			yield departure

class GtfsDepartures(object):
	def __init__(self, directory, timezone):
		self.stop_times = StopTimes(directory)
		self.directory = directory
		self.depiter = gtfs_departures(directory, self.stop_times, timezone)

		stopreader = NamedTupleCsvReader(bomopen(
			os.path.join(directory, 'stops.txt')))

		self.stops = {r.stop_id: r for r in stopreader}
		
	def __iter__(self):
		return self.depiter
