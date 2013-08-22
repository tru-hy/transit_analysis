#!/usr/bin/env python2

import sys
import os
from itertools import chain, groupby
from collections import defaultdict

from transit_analysis.config import coordinate_projection
from transit_analysis.utils.gtfs import *
from transit_analysis import recordtypes as rec
from transit_analysis.utils.dumping import csvmapper, csv_dump

def get_reader(gtfs_dir, f):
	filepath = os.path.join(gtfs_dir, f)
	return NamedTupleCsvReader(bomopen(os.path.join(gtfs_dir, f)))

csv_commands = []
def csv(func):
	csv_commands.append(csv_dump(func))
	return func
@csv
def coordinate_shape(gtfs_dir, **kwargs):
	import numpy as np
	import pyproj
	proj = pyproj.Proj(init=coordinate_projection)
	filepath = os.path.join(gtfs_dir, 'shapes.txt')
	shapes = {}
	for row in NamedTupleCsvReader(bomopen(filepath)):
		if row.shape_id not in shapes:
			shapes[row.shape_id] = []
		shapes[row.shape_id].append((
			int(row.shape_pt_sequence),
			float(row.shape_pt_lat),
			float(row.shape_pt_lon)))
	
	for shape_id, coords in shapes.iteritems():
		# Could use a heap if this causes
		# performance problems (probably wont)
		coords.sort()
		lat, lon = zip(*coords)[1:]
		latlon = zip(lat, lon)

		projected = np.array(proj(lon, lat)).T
		diffs = np.sqrt(np.sum(np.diff(projected, axis=0)**2, axis=1))
		distances = [0.0] + list(np.cumsum(diffs))
		yield rec.coordinate_shape(
			shape=shape_id,
			coordinates=latlon,
			distances=distances)

@csv
def transit_stop(gtfs_dir, **kwargs):
	filepath = os.path.join(gtfs_dir, 'stops.txt')
	for row in NamedTupleCsvReader(bomopen(filepath)):
		yield rec.transit_stop(
			stop_id=row.stop_id,
			stop_name=row.stop_name,
			latitude=row.stop_lat,
			longitude=row.stop_lon)

@csv
def transit_schedule_shape(gtfs_dir, **kwargs):
	trips = get_reader(gtfs_dir, 'trips.txt')
	for t in trips:
		yield rec.transit_schedule_shape(
			schedule_id=t.trip_id,
			shape_id=t.shape_id)

@csv
def transit_schedule_stop(gtfs_dir, **kwargs):
	rows = iter(get_reader(gtfs_dir, 'stop_times.txt'))
	
	def to_seconds(time):
		h, m, s = time.split(':')
		return int(h)*60*60 + int(m)*60 + int(s)
	
	trip = None
	for row in rows:
		if row.trip_id != trip:
			assert row.stop_sequence == "1"
			trip = row.trip_id
			start_time = to_seconds(row.departure_time)
		arrival = to_seconds(row.arrival_time) - start_time
		departure = to_seconds(row.departure_time) - start_time
		yield rec.transit_schedule_stop(
			schedule_id=row.trip_id,
			stop_id=row.stop_id,
			arrival=arrival,
			departure=departure)

@csv
def transit_shape_stop(gtfs_dir, **kwargs):
	tripidx = get_reader(gtfs_dir, 'trips.txt')
	tripidx = {t.trip_id: t for t in tripidx}
	rows = iter(get_reader(gtfs_dir, 'stop_times.txt'))
	sequences = {}
	
	trip = None
	prev_stop = None
	
	for tid, trips in groupby(rows, lambda x: x.trip_id):
		sid = tripidx[tid].shape_id
		stops = [t.stop_id for t in trips]
		if sid not in sequences:
			sequences[sid] = stops
			continue
		if sequences[sid] == stops:
			continue
		print >>sys.stderr,\
			"Different stop sequences for a shape,"\
			"selecting longest"
		if len(stops) > len(sequences[sid]):
			sequences[sid] = stops
	
	for shape_id, seq in sequences.iteritems():
		prev = None
		yield rec.transit_shape_stop(
			shape_id=shape_id,
			stop_ids=seq,
			sequence=range(len(seq)))

@csv
def transit_departure(gtfs_dir, adapter, **kwargs):
	import imp
	adapter = imp.load_source("adapter", adapter)
	timezone = adapter.timezone
	departures = GtfsDepartures(gtfs_dir, timezone)
	handler = adapter.GtfsDeparture(departures)
	seen_ids = {}
	
	for departure in departures:
		record = handler(departure)
		# Hacking as this can't be done as a default in
		# postgres
		depid = record.departure_id
		if depid in seen_ids:
			print >>sys.stderr, "Ignoring duplicate", depid
			continue
		seen_ids[depid] = True

		record = list(record)
		yield rec.transit_departure(*record)

if __name__ == '__main__':
	import argh
        parser = argh.ArghParser()
        parser.add_commands(csv_commands)
	parser.dispatch()

