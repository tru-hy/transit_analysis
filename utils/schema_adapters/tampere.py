import sys
import datetime
import re
from collections import OrderedDict

import pytz
import dateutil.parser


from transit_analysis.recordtypes import *

timezone = "Europe/Helsinki"

def get_departure_id(route, direction, departure_time_iso):
	return "%s/%s/%s"%(route, direction, departure_time_iso)

def _get_utc_iso(date, departure_time, timezone):
	year, month, day = map(int, date.split('-'))
	hour, minute = int(departure_time[:2]), int(departure_time[2:])

	leftover = None
	if hour > 23:
		leftover = hour - 23
		hour = 23

	time = datetime.datetime(year, month, day, hour, minute)
	time = timezone.normalize(timezone.localize(time))
	
	if leftover is not None:
		time += datetime.timedelta(hours=leftover)
	time = time.astimezone(pytz.UTC)
	return time.isoformat()[:-6]

#isotsparse =  2013-07-08T22:20:00.005+03:00
def fastisots(t):
	parts = map(int, (t[0:4], t[5:7], t[8:10], t[11:13], t[14:16], t[17:19]))
	frac = t[19:-6]
	if frac:
		msecs = int(round(float(frac)*1e6))
		parts.append(msecs)
	
	dt = datetime.datetime(*parts)
	offset = t[-6:]
	sign = offset[0]
	offset = int(sign+offset[1:3])*60 + int(sign+offset[4:])
	ts = datetime.datetime(*parts)
	ts -= datetime.timedelta(minutes=offset)
	return ts

class SiriDepartureMeasurement:
	def __init__(self, timezone=timezone, **kwargs):
		self.timezone = pytz.timezone(timezone)
		self.departure_cache = OrderedDict()
		self.largetimecache = OrderedDict()
		self.dateutilutc = dateutil.tz.tzutc()
	
	def departure(self, act):
		route = act["LineRef"]
		direction = act["DestinationName"]
		date = act["DataFrameRef"]
		departure_time = act["DatedVehicleJourneyRef"]
		
		key = (route, direction, departure_time)
		if key in self.departure_cache:
			return self.departure_cache[key]

		departure_time = _get_utc_iso(date, departure_time, self.timezone)
		
		dep_id = get_departure_id(route, direction, departure_time)
		self.departure_cache[key] = dep_id
		if len(self.departure_cache) > 100:
			self.departure_cache.popitem(False)
		return dep_id
		
	
	def measurement(self, act):
		timestamp = act["RecordedAtTime"]
		latitude = float(act["Latitude"])
		longitude = float(act["Longitude"])
		bearing = float(act["Bearing"])
		source = act["VehicleRef"].strip()
		
		if timestamp not in self.largetimecache:
			ts = fastisots(timestamp).isoformat()
			self.largetimecache[timestamp] = ts
			if len(self.largetimecache) > 20:
				self.largetimecache.popitem(False)

			timestamp = ts
		else:
			timestamp = self.largetimecache[timestamp]

		

		measurement = coordinate_measurement(
			source=source,
			time=timestamp,
			latitude=latitude,
			longitude=longitude,
			bearing=bearing)

		return measurement
	
	def __call__(self, act):
		departure_id = self.departure(act)
		measurement = self.measurement(act)

		return departure_id, measurement

class GtfsDeparture:
	def __init__(self, departures):
		self.stop_times = departures.stop_times
		self.stops = departures.stops

	def __call__(self, dep):
		dest_id = self.stop_times[dep.trip_id][-1].stop_id
		dest_name = self.stops[dest_id].stop_name.strip()
		dep_id = get_departure_id(
			dep.route_id,
			dest_name,
			dep.departure_time.isoformat()
			)
		return transit_departure(
			departure_id=dep_id,
			route_name=dep.route_id,
			route_variant=dep.route_id,
			direction=dest_name,
			shape=dep.shape_id,
			departure_time=dep.departure_time,
			schedule_id=dep.trip_id)
