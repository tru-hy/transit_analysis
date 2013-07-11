import sys
import datetime
import re

import pytz
import dateutil.parser

from transit_analysis.recordtypes import *

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
	def __init__(self, timezone="Europe/Helsinki", **kwargs):
		self.timezone = pytz.timezone(timezone)
		self.timecache = {}
		self.dateutilutc = dateutil.tz.tzutc()
	
	def __call__(self, act):
		def getfield(name):
			return act.iter(name).next().text
		route = getfield("LineRef")
		direction = str(int(getfield("DirectionRef")) - 1)
		date = getfield("DataFrameRef")
		departure_time = getfield("DatedVehicleJourneyRef")
		timestamp = getfield("RecordedAtTime")
		latitude = float(getfield("Latitude"))
		longitude = float(getfield("Longitude"))
		bearing = float(getfield("Bearing"))
		source = getfield("VehicleRef").strip()
		
		timestamp = fastisots(timestamp).isoformat()
		timekey = (date, departure_time)
		if timekey in self.timecache:
			departure_time = self.timecache[timekey]
		else:
			departure_time = _get_utc_iso(date, departure_time, self.timezone)
			self.timecache[timekey] = departure_time

		departure_id = get_departure_id(route, direction, departure_time)

		measurement = coordinate_measurement(
			source=source,
			time=timestamp,
			latitude=latitude,
			longitude=longitude,
			bearing=bearing)


		return departure_id, measurement
