import sys
import os
from transit_analysis.utils.gtfs import *
from transit_analysis.recordtypes import transit_departure
from transit_analysis.utils.dumping import csvmapper

def shapes_csv(gtfs_dir):
	import numpy as np
	import pyproj
	proj = pyproj.Proj(init="EPSG:3857")
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
		print "\t".join(map(csvmapper, (shape_id, latlon, distances)))



def departures_csv(adapter, gtfs_dir):
	import imp
	adapter = imp.load_source("adapter", adapter)
	timezone = adapter.timezone
	departures = GtfsDepartures(gtfs_dir, timezone)
	handler = adapter.GtfsDeparture(departures)
	hack = transit_departure('dummy')
	trace_idx = hack._fields.index('trace')
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

		record = list(handler(departure))
		if record[trace_idx] is None:
			record[trace_idx] = "transit_departure/"+depid
		record_str = "\t".join(map(csvmapper, record))
		print record_str



if __name__ == '__main__':
	import argh
        parser = argh.ArghParser()
        parser.add_commands([departures_csv, shapes_csv])
	parser.dispatch()

