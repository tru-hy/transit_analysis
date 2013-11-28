#!/usr/bin/env python2

import sys

import numpy as np
from psycopg2.extras import NamedTupleCursor
import pyproj
import scipy.interpolate


from transit_analysis import schema, config, recordtypes
from transit_analysis.ext.obs_splines import \
	smooth1d_grid_l1_l2_missing, smooth1d_grid_l1_l2
from transit_analysis.ext.pymapmatch.fastroutematch import RouteMatcher2d

coord_proj = pyproj.Proj(init=config.coordinate_projection)

class RouteException(Exception): pass

def to_progress_inplace(dist):
	maxseen = dist[0]
	for i in range(1, len(dist)):
		if ~np.isfinite(dist[i]):
			continue

		if dist[i] < maxseen:
			dist[i] = maxseen
		else:
			maxseen = dist[i]


def get_shape_departures(db, shape):
	conn = db.bind.raw_connection()
	dep_cursor = conn.cursor(cursor_factory=NamedTupleCursor)
	
	# Select "finalised" departures
	# which haven't been processed.
	to_process = """
	select departure_id from coordinate_measurement
	where finalized = true
	and unfilterable = false
	"""

	q = """
	select *
	from transit_departure
	where departure_id in (%s)
	and shape=%%s
	and routed_trace isnull
	"""%(to_process,)
	
	dep_cursor.execute(q, (shape,))
		
	coord_cur = conn.cursor(cursor_factory=NamedTupleCursor)
	for departure in dep_cursor:
		coord_cur.execute("""
			select
			time, latitude, longitude, start_time
			from coordinate_measurement
			where
				departure_id=%s
			limit 1;
			""", (departure.departure_id,)
			)
		data = iter(coord_cur).next()
		# Make the timestamp to point to the scheduled
		# departure time.
		offset = (data.start_time - departure.departure_time).total_seconds()
		time = [t+offset for t in data.time]
		
		yield departure, zip(time, data.latitude, data.longitude)


class RouteFilter:
	def __init__(self, waypoints, new_dt=1.0, new_dd=5.0):
		self.matcher = RouteMatcher2d(waypoints)
		self.distances = self.matcher.distances
		self.new_dt = new_dt
		self.new_dd = new_dd
	
	def __call__(self, ts, coords, departure, **kwargs):
		ts, coords = self.matcher(ts, coords)
		if len(ts) < 10:
			return None
	
		dist_interp = scipy.interpolate.interp1d(ts, coords)
		new_ts = np.arange(ts[0], ts[-1] - self.new_dt, self.new_dt)
		if len(new_ts) < 10:
			return None
		gridded_dist = dist_interp(new_ts)
		smoothed_dist = smooth1d_grid_l1_l2(gridded_dist, smoothing=3.0,
				crit=1e-3, max_iters=30)
	
		to_progress_inplace(smoothed_dist)
		
		dist_to_ts = scipy.interpolate.interp1d(smoothed_dist, new_ts, bounds_error=False)
		mono_route_distance = np.arange(0, self.distances[-1], self.new_dd)
		mono_ts = dist_to_ts(mono_route_distance)
		time_spent = np.diff(mono_ts)

		route_speed = np.diff(smoothed_dist)/self.new_dt
		smoothed_dist = smoothed_dist[1:]
		
		record = recordtypes.routed_trace(
			reference_time=departure.departure_time,
			shape=departure.shape,
			timestamp=new_ts,
			route_distance=smoothed_dist,
			route_speed=route_speed,
			time_at_distance_grid=mono_ts.astype(np.float32).data,
			distance_bin_width=self.new_dd)
		return record


def filter_shape_routes(db, shape, filter_cls=RouteFilter):
	conn = db.bind.raw_connection()
	
	
	shapetbl = db.tables['coordinate_shape']
	shape_data = shapetbl.select().where(shapetbl.c.shape==shape).execute()
	_, shape_data, shape_dist, nodes = shape_data.fetchone()
	
	
	lat, lon = zip(*shape_data)
	shape_cart = np.array(coord_proj(lon, lat)).T
	routefilter = filter_cls(shape_cart)
	
	coord_cur = conn.cursor()
	
	for i, (departure, coordinates) in enumerate(get_shape_departures(db, shape)):
		ts, lat, lon = np.array(coordinates).T
		cart = np.array(zip(*coord_proj(lon, lat)))
		# The dumper shouldn't put out identical timestamps, but
		# apparently it does.
		valid = np.flatnonzero(np.diff(ts) > 0)
		if np.sum(valid) < 10:
			yield departure, None
		try:
			result = routefilter(ts[valid], cart[valid], departure=departure)
			yield departure, result
		except RouteException:
			yield departure, None


def insert_routed_trace(db, routed_tbl, departure_tbl, mapping):
	departure, trace = mapping
	if not trace:
		db.bind.execute("""
		update coordinate_measurement set unfilterable=true
		where departure_id=%s""", departure.departure_id)
		return

	trace = trace._asdict()
	del trace['id']

	if departure.routed_trace:
		departure_tbl.update().\
			where(departure_tbl.c.departure_id == departure.departure_id).\
			values(routed_trace=None).execute()
		routed_tbl.delete().where(routed_tbl.c.id == departure.routed_trace)

	result = routed_tbl.insert(trace).execute()
	departure_tbl.update().\
		where(departure_tbl.c.departure_id == departure.departure_id).\
		values(routed_trace=result.inserted_primary_key[0]).execute()

def _fetch_shapes(conn):
	cur = conn.cursor()
	# Select shapes for which there is data to process
	q = """
	select distinct shape
	from transit_departure
	where routed_trace isnull
	and departure_id in
		(select distinct departure_id from coordinate_measurement where finalized = true);
	"""
	cur.execute(q)
	return [r[0] for r in cur]


def filter_routes(uri=schema.default_uri, shape=None, verbose=False):
	db = schema.connect(uri)
	conn = db.bind.raw_connection()
	
	if shape is not None:
		shapes = [shape]
	else:
		shapes = list(_fetch_shapes(conn))
	
	routed_tbl = db.tables['routed_trace']
	departure_tbl = db.tables['transit_departure']
	n_shapes = len(shapes)

	total_fits = 0
	for i, shape in enumerate(shapes):
		#if verbose:
		#	print >>sys.stderr, "Processing %i/%i shapes"%(i+1, n_shapes)
		mappings = filter_shape_routes(db, shape)

		for mapping in mappings:
			insert_routed_trace(db, routed_tbl, departure_tbl, mapping)
			total_fits += 1
	if verbose:
		print >>sys.stderr, "%i new traces"%(total_fits,)

def plot_shape_route_filtering(db, shape):
	import matplotlib.pyplot as plt
	
	res = filter_shape_routes(db, shape)
	distgrid = None
	i = -1
	for i, (departure, record) in enumerate(res):
		if distgrid is None:
			distgrid = np.arange(0,
				record.distance_bin_width*len(record.time_at_distance_grid),
				record.distance_bin_width)
		#plt.plot(distgrid, record.time_at_distance_grid)
		plt.plot(distgrid[1:], np.diff(distgrid)/np.diff(record.time_at_distance_grid), color='black', alpha=0.2)
	plt.show()
	return i + 1

def plot_route_filtering(uri=schema.default_uri, shape=None):
	"""Used mainly for testing"""
	import time

	db = schema.connect(uri)
	conn = db.bind.raw_connection()
	
	if shape is not None:
		shapes = [shape]
	else:
		shapes = list(_fetch_shapes(conn,))
	
	for shape in shapes:
		plot_shape_route_filtering(db, shape)

def filter_stop_sequences(uri=schema.default_uri):
	db = schema.connect(uri)
	con = db.bind

	q = """
	select shape, coordinates
	from coordinate_shape
	where shape in
	(select distinct shape_id from transit_shape_stop)
	"""
	shapes = con.execute(q)
	for shape in shapes:
		filter_shape_stop_sequences(db, shape)
	
def filter_shape_stop_sequences(db, shape):
	q = """
	with ss as (
	select
	 unnest(stop_ids) as stop_id,
	 unnest(sequence) as sequence
	from transit_shape_stop
	where shape_id=%(shape)s
	)
	select ss.*, latitude, longitude
	from ss
	join transit_stop as s on s.stop_id=ss.stop_id
	order by sequence
	"""
	# This stuff shouldn't be this damn hard!
	res = db.bind.execute(q, shape=shape.shape)
	data = zip(*res.fetchall())
	data = dict(zip(res.keys(), data))

	sx, sy = coord_proj(*zip(*shape.coordinates)[::-1])
	# The weirdish std parameters ensure that the matching
	# is done basically so that no (relative) penalty comes
	# from long jumps, which is what we want as the "timestamp"
	# really isn't.
	matcher = RouteMatcher2d(zip(sx, sy),
		measurement_std=1.0,
		transition_std=1e6)

	x, y = coord_proj(data['longitude'], data['latitude'])

	seq, distances = matcher(np.array(data['sequence'], dtype=float), zip(x, y))

	tbl = db.tables['transit_shape_stop']
	tbl.update().values(distances=distances).where(tbl.c.shape_id==shape.shape).execute()



if __name__ == '__main__':
	import argh
        parser = argh.ArghParser()
        parser.add_commands([filter_routes, plot_route_filtering, filter_stop_sequences])
        parser.dispatch()

