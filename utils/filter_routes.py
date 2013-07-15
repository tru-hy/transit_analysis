import sys
from ctypes import c_double, byref

import numpy as np
from psycopg2.extras import NamedTupleCursor
from shapely.geometry import Point, LineString, asMultiPoint
from shapely.geometry.point import update_point_from_py
from shapely.geos import lgeos
import pyproj
import scipy.interpolate
import scipy.signal
import scipy.ndimage

from transit_analysis import schema
from transit_analysis.ext.obs_splines import \
	smooth1d_grid_l1_l2_missing, smooth1d_grid_l1_l2

coord_proj = pyproj.Proj(init="EPSG:3857")

class RouteException(Exception): pass

def out_of_route(err):
	med_err = scipy.signal.medfilt(err, 31)
	return med_err > 100

def find_valid_blocks(invalid):
	block_changes = [0] + list(np.flatnonzero(np.ediff1d(invalid) != 0))
	if invalid[block_changes[0]]:
		block_changes = block_changes[1:]
	block_changes.append(len(invalid))

	valid_blocks = []
	for i in range(0, len(block_changes)-1, 2):
		block = slice(block_changes[i], block_changes[i+1])
		valid_blocks.append(block)
		
	return valid_blocks

def find_approx_monotonic_blocks(dx):
	smooth_diff_sign = np.sign(scipy.ndimage.gaussian_filter1d(dx, 101))

	changes = list(np.flatnonzero(np.diff(smooth_diff_sign)))
	changes.insert(0, 0)
	changes.append(len(dx))
	return (slice(changes[i], changes[i+1]) for i in range(0, len(changes)-1)
		if changes[i+1]-changes[i] > 2)

def project_to_linestring(cart, shape_string):
	# Shapely's type mangling is dog slow, so let's
	# use the lowlevel api by hand
	point = Point([0, 0])
	cs = lgeos.GEOSGeom_getCoordSeq(point._geom)
	xd = c_double(0)
	yd = c_double(0)
	for i in range(len(cart)):
		cx = cart[i][0]
		cy = cart[i][1]
		lgeos.GEOSCoordSeq_setX(cs, 0, c_double(cx))
		lgeos.GEOSCoordSeq_setY(cs, 0, c_double(cy))
		
		dist = lgeos.GEOSProject(shape_string._geom, point._geom)
		interp_geom = lgeos.GEOSInterpolate(shape_string._geom, dist)
		interp_cs = lgeos.GEOSGeom_getCoordSeq(interp_geom)
		
		lgeos.GEOSCoordSeq_getX(interp_cs, 0, byref(xd))
		lgeos.GEOSCoordSeq_getY(interp_cs, 0, byref(yd))
		x = xd.value
		y = yd.value
		yield dist, x, y

def map_departure_to_route(ts, cart, shape_string, new_dt=1.0,
		raw_data_callback=None):
	min_werr = 0.1
	
	route_mapping = np.array(list(project_to_linestring(cart, shape_string)))
	route_distance = route_mapping[:,0]
	on_string = route_mapping[:,1:]
	err = np.sqrt(np.sum((on_string - cart)**2, axis=1))

	invalid = out_of_route(err)
	valid_blocks = find_valid_blocks(invalid)
	blocks = []

	for block in valid_blocks:
		#werr = err[block].copy()
		#werr[werr < min_werr] = min_werr
		#w = np.sqrt(100*1.0/werr)
		n = block.stop - block.start
		if n < 3: continue
		new_ts = np.arange(ts[block.start], ts[block.stop-1], new_dt)
		
		gridded_dist = scipy.interpolate.interp1d(ts[block], route_distance[block])
		gridded_dist = gridded_dist(new_ts)
		# TODO: Give out some kind of quality estimate using "interpolationess",
		#	ie. distance from the interpolation points used for each gridded
		#	measurement.
		# TODO: Doing the blocking without the smoothing would
		#	be a lot faster
		# TODO: See if a (non-binary) weighted formulation can be done for
		#	the L1-L2 smoothing
		smoothed_dist = smooth1d_grid_l1_l2(gridded_dist, smoothing=3.0,
					crit=1e-3, max_iters=100)

		mono = (find_approx_monotonic_blocks(np.diff(smoothed_dist)*new_dt))
		mono = ((new_ts[m], smoothed_dist[m]) for m in mono)
		blocks.extend(mono)
	
	if len(blocks) == 0:
		raise RouteException("No valid blocks found")

	# Get block of which start time is closest to the
	# departure time (assuming ts is relative to the departure time)
	
	new_ts, fitted_route_distance = blocks[np.argmin([b[0][0] for b in blocks])]
	
	if raw_data_callback is not None:	
		raw_block = slice(*ts.searchsorted([new_ts[0], new_ts[-1]]))
	
		raw_data_callback(ts=ts[raw_block], cart=cart[raw_block],
			route_distance=route_distance[raw_block])

	route_speed = np.diff(fitted_route_distance)*new_dt
	fitted_route_distance = fitted_route_distance[1:]
	block = slice(block.start+1, block.stop)
	if np.mean(route_speed) < 0:
		# The route should be driven in the presented direction
		raise RouteException("Wrong driving direction")

	new_ts = new_ts[1:]
	return new_ts, fitted_route_distance, route_speed

def filter_shape_routes(db, shape, raw_data_callback=lambda **kwargs: None):
	conn = db.bind.raw_connection()
	
	dep_cursor = conn.cursor(cursor_factory=NamedTupleCursor)
	dep_cursor.execute("""
		select *
		from transit_departure
		left join coordinate_trace on trace=id
		where shape=%s
		""", (shape,))

	shapetbl = db.tables['coordinate_shape']
	shape_data = shapetbl.select().where(shapetbl.c.shape==shape).execute()
	shape_data = shape_data.fetchone()[1]

	lat, lon = zip(*shape_data)
	shape_cart = coord_proj(lon, lat)
	shape_string = LineString(zip(*shape_cart))

	coord_cur = conn.cursor()
	
	for i, departure in enumerate(dep_cursor):
		coord_cur.execute("""
			select distinct extract(epoch from time-%s) as ts,
				latitude, longitude
			from coordinate_measurement
			where
				source=%s AND
				time between %s and %s
			order by ts
			""", (departure.departure_time,
				departure.source,
				departure.start_time,
				departure.end_time))
		
		coordinates = coord_cur.fetchall()
		if len(coordinates) < 10: continue
		ts, lat, lon = np.array(coordinates).T
		cart = np.array(zip(*coord_proj(lon, lat)))
		try:
			yield departure, map_departure_to_route(ts, cart, shape_string,
				raw_data_callback=raw_data_callback)
		except RouteException:
			continue

def insert_routed_trace(routed_tbl, departure_tbl, mapping):
	departure, trace = mapping
	
	result = routed_tbl.insert(dict(
			reference_time=departure.departure_time,
			shape=departure.shape,
			timestamp=trace[0],
			route_distance=trace[1],
			route_speed=trace[2],
			)).execute()
	
	departure_tbl.update().\
		where(departure_tbl.c.departure_id == departure.departure_id).\
		values(routed_trace=result.inserted_primary_key[0]).execute()

def _fetch_shapes(conn):
	cur = conn.cursor()
	cur.execute("""
		select distinct shape
		from transit_departure
		where trace notnull
		""")
	
	return [r[0] for r in cur]


def filter_routes(uri=schema.default_uri, shape=None):
	db = schema.connect(uri)
	conn = db.bind.raw_connection()
	
	if shape is not None:
		shapes = [shape]
	else:
		shapes = list(_fetch_shapes(conn))
	
	routed_tbl = db.tables['routed_trace']
	departure_tbl = db.tables['transit_departure']
	n_shapes = len(shapes)
	for i, shape in enumerate(shapes):
		print >>sys.stderr, "Processing %i/%i shapes"%(i+1, n_shapes)
		mappings = filter_shape_routes(db, shape)
		for mapping in mappings:
			insert_routed_trace(routed_tbl, departure_tbl, mapping)

def plot_shape_route_filtering(db, shape):
	import matplotlib.pyplot as plt
	
	# Hacking the lack of nonlocal
	rawdata = {}
	def rawdata_callback(**kwargs):
		rawdata.update(**kwargs)

	for departure, (ts, dist, speed) in filter_shape_routes(db, shape, raw_data_callback=rawdata_callback):
		#plt.plot(dist, speed, color='black', alpha=0.1)
		plt.plot(ts, dist, color='black', alpha=0.1)
		#plt.subplot(2,1,2)
		#rawspeed = np.diff(rawdata['route_distance'])/np.diff(rawdata['ts'])
		#plt.plot(rawdata['ts'][1:], rawspeed*3.6, 'r', label='raw')
		#plt.plot(ts, speed*3.6, 'b', label='smoothed')
		#plt.subplot(2,1,1)
		#plt.plot(rawdata['ts'], rawdata['route_distance'], 'r', label='raw')
		#plt.plot(ts, dist, 'b', label='smoothed')
	plt.show()

def plot_route_filtering(uri=schema.default_uri, shape=None):
	"""Used mainly for testing"""

	db = schema.connect(uri)
	conn = db.bind.raw_connection()
	
	if shape is not None:
		shapes = [shape]
	else:
		shapes = list(_fetch_shapes(conn))
	
	for shape in shapes:
		plot_shape_route_filtering(db, shape)


if __name__ == '__main__':
	import argh
        parser = argh.ArghParser()
        parser.add_commands([filter_routes, plot_route_filtering])
        parser.dispatch()

