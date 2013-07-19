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

from transit_analysis import schema, config, recordtypes
from transit_analysis.ext.obs_splines import \
	smooth1d_grid_l1_l2_missing, smooth1d_grid_l1_l2

coord_proj = pyproj.Proj(init=config.coordinate_projection)

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

def find_approx_increasing_blocks(dx):
	smooth_diff = scipy.ndimage.gaussian_filter1d(dx, 101)

	valid = smooth_diff >= 0
	
	i = 0
	n = len(smooth_diff)
	blocks = []
	while True:
		for i in range(i, n):
			if valid[i]:
				blockstart = i
				break
		else:
			break

		for i in range(i, n):
			if not valid[i]:
				block = slice(blockstart, i)
				blocks.append(block)
				break
		else:
			blocks.append(slice(blockstart, None))
			break
	return blocks

def monotonize(x, y):
	# TODO: Do this more elegantly/robustly. And less slowly.
	# In optimal case in the fitting itself
	y = y.copy()
	
	while True:
		regressing = np.flatnonzero(np.ediff1d(y, to_begin=[0]) < 0)
		if len(regressing) == 0: break
		y[regressing] = y[regressing - 1]
	return x, y


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

def map_departure_to_route(ts, cart, shape_string, departure, new_dt=1.0,
		new_dd=10.0,
		raw_data_callback=None):
	min_werr = 0.1
	
	maxlength = int(shape_string.length) + 1
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
		if n < 10: continue
		new_ts = np.arange(ts[block.start], ts[block.stop-1], new_dt)
		
		# TODO: Give out some kind of quality estimate using "interpolationess",
		#	ie. distance from the interpolation points used for each gridded
		#	measurement.
		#reference_points = scipy.interpolate.interp1d(ts[block], ts[block], kind='nearest')
		#ref_dist = np.abs(reference_points(new_ts) - new_ts)

		gridded_dist = scipy.interpolate.interp1d(ts[block], route_distance[block])
		gridded_dist = gridded_dist(new_ts)
		# TODO: Doing the blocking without the smoothing would
		#	be a lot faster
		# TODO: See if a (non-binary) weighted formulation can be done for
		#	the L1-L2 smoothing
		smoothed_dist = smooth1d_grid_l1_l2(gridded_dist, smoothing=3.0,
					crit=1e-3, max_iters=100)

		mono = (find_approx_increasing_blocks(np.diff(smoothed_dist)*new_dt))
		mono = ((new_ts[m], smoothed_dist[m]) for m in mono)
		blocks.extend(mono)
	
	if len(blocks) == 0:
		raise RouteException("No valid blocks found")

	# Get block of which start time is closest to the
	# departure time (assuming ts is relative to the departure time)
	
	new_ts, fitted_route_distance = blocks[np.argmin([b[0][0] for b in blocks])]
	#new_ts, fitted_route_distance = monotonize(new_ts, fitted_route_distance)
	#dist_to_ts = scipy.interpolate.interp1d(fitted_route_distance, new_ts, bounds_error=False)
	#fitted_route_distance = np.arange(0, maxlength, new_dd)
	#new_ts = dist_to_ts(fitted_route_distance)
	
	"""
	nanstart = 0
	for nanstart in range(len(new_ts)):
		if np.isfinite(new_ts[nanstart]): break
	for nanend in range(len(new_ts)-1, 0, -1):
		if np.isfinite(new_ts[nanend]): break
	"""
	
	if raw_data_callback is not None:	
		raw_block = slice(*ts.searchsorted([new_ts[0], new_ts[-1]]))
	
		raw_data_callback(ts=ts[raw_block], cart=cart[raw_block],
			route_distance=route_distance[raw_block], raw_cart=cart,
			shape_string=shape_string, err=err[raw_block])

	route_speed = np.diff(fitted_route_distance)/(new_dt)
	fitted_route_distance = fitted_route_distance[1:]
	block = slice(block.start+1, block.stop)

	new_ts = new_ts[1:]
	
	mono_ts, mono_route_distance = monotonize(new_ts, fitted_route_distance)
	dist_to_ts = scipy.interpolate.interp1d(fitted_route_distance, new_ts, bounds_error=False)
	mono_route_distance = np.arange(0, maxlength, new_dd)
	mono_ts = dist_to_ts(mono_route_distance)
	time_spent = np.diff(mono_ts)
	
	record = recordtypes.routed_trace(
		reference_time=departure.departure_time,
		shape=departure.shape,
		timestamp=new_ts,
		route_distance=fitted_route_distance,
		route_speed=route_speed,
		time_at_distance_grid=mono_ts,
		distance_bin_width=new_dd)
		
	return record

def get_shape_departures(db, shape, override):
	conn = db.bind.raw_connection()
	dep_cursor = conn.cursor(cursor_factory=NamedTupleCursor)
	
	dep_cursor = conn.cursor(cursor_factory=NamedTupleCursor)
	q = """
		select *
		from transit_departure
		right join coordinate_trace on trace=id
		where shape=%s
		"""
	if not override:
		q += "and routed_trace is null"
	
	dep_cursor.execute(q, (shape,))
		
	coord_cur = conn.cursor()
	for departure in dep_cursor:
		coord_cur.execute("""
			select distinct extract(epoch from time-%s) as ts,
				latitude, longitude
			from coordinate_measurement
			where
				source=%s AND
				time between %s and %s AND
				latitude != 0 AND
				longitude != 0
			order by ts
			""", (departure.departure_time,
				departure.source,
				departure.start_time,
				departure.end_time))
		yield departure, coord_cur.fetchall()


def filter_shape_routes(db, shape, override,
		raw_data_callback=lambda **kwargs: None):
	conn = db.bind.raw_connection()
	
	
	shapetbl = db.tables['coordinate_shape']
	shape_data = shapetbl.select().where(shapetbl.c.shape==shape).execute()
	_, shape_data, shape_dist = shape_data.fetchone()
	
	
	lat, lon = zip(*shape_data)
	shape_cart = coord_proj(lon, lat)
	shape_string = LineString(zip(*shape_cart))
	
	coord_cur = conn.cursor()
	
	for i, (departure, coordinates) in enumerate(get_shape_departures(db, shape, override)):
		if len(coordinates) < 10: continue
		ts, lat, lon = np.array(coordinates).T
		cart = np.array(zip(*coord_proj(lon, lat)))
		
		try:
			yield departure, map_departure_to_route(ts, cart, shape_string,
				departure,
				raw_data_callback=raw_data_callback)
		except RouteException:
			continue


def insert_routed_trace(routed_tbl, departure_tbl, mapping):
	departure, trace = mapping
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

def _fetch_shapes(conn, override):
	cur = conn.cursor()
	q = """
		select distinct shape
		from transit_departure
		where trace notnull
		"""
	if not override:
		q += "and routed_trace isnull"
	cur.execute(q)
	
	return [r[0] for r in cur]


def filter_routes(uri=schema.default_uri, shape=None, override=False):
	db = schema.connect(uri)
	conn = db.bind.raw_connection()
	
	if shape is not None:
		shapes = [shape]
	else:
		shapes = list(_fetch_shapes(conn, override))
	
	routed_tbl = db.tables['routed_trace']
	departure_tbl = db.tables['transit_departure']
	n_shapes = len(shapes)
	for i, shape in enumerate(shapes):
		print >>sys.stderr, "Processing %i/%i shapes"%(i+1, n_shapes)
		mappings = filter_shape_routes(db, shape, override)
		for mapping in mappings:
			insert_routed_trace(routed_tbl, departure_tbl, mapping)

def plot_shape_route_filtering(db, shape, override):
	import matplotlib.pyplot as plt
	import scipy.ndimage
	
	# Hacking the lack of nonlocal
	rawdata = {}
	def rawdata_callback(**kwargs):
		rawdata.update(**kwargs)
	shapeplot = None
	timespent_grid = None
	i = -1
	res = filter_shape_routes(db, shape, override, raw_data_callback=rawdata_callback)
	for i, (departure, record) in enumerate(res):
		#rawcart = rawdata['raw_cart']
		#if not shapeplot:
		#	shapeplot = True
		#rawts = rawdata['ts']
		#rawdist = rawdata['route_distance']
		plt.xlim(0, rawdata['shape_string'].length)
		
		#plt.plot(np.isfinite(ts))
		#plt.plot(ts, dist, '.')
		#plt.plot(dist, speed*3.6)
		#plt.plot(x, y, '.')
		#ax = plt.subplot(2,1,1)
		#plt.plot(rawdata['shape_string'].xy[0], rawdata['shape_string'].xy[1])
		#plt.plot(rawdata['cart'][:,0], rawdata['cart'][:,1], '.')
		#plt.show()
		#plt.plot(rawdata['ts'], rawdata['route_distance'], '.')
		#plt.plot(dist, speed)
		ax = plt.subplot(1,1,1)
		#ax.set_yscale('log', basey=2)
		#plt.plot(dist, ts, color='black')
		time_at_dist = record.time_at_distance_grid
		distgrid = np.arange(0, len(time_at_dist))*record.distance_bin_width
		#plt.plot(distgrid, time_at_dist)
		plt.plot(record.route_distance, record.route_speed)
		#if timespent_grid is None:
		#	timespent_grid = timespent.copy()
		#	timespent_grid[np.isnan(timespent_grid)] = 0.0
		#else:
		#	valid = np.isfinite(timespent)
		#	timespent_grid[valid] += timespent[valid]
		#plt.plot(rawdata['ts'][1:], np.diff(rawdata['ts']))
		#plt.plot(rawdata['ts'], rawdata['err'])
		#plt.show()
		#plt.plot(dist, speed, color='black', alpha=0.1)
		#plt.plot(ts, dist, color='black', alpha=0.1)
		#plt.subplot(2,1,2)
		#rawspeed = np.diff(rawdata['route_distance'])/np.diff(rawdata['ts'])
		#plt.plot(rawdata['ts'][1:], rawspeed*3.6, 'r', label='raw')
		#plt.plot(ts, speed*3.6, 'b', label='smoothed')
		#plt.subplot(2,1,1)
		#plt.plot(rawdata['ts'], rawdata['route_distance'], 'r', label='raw')
		#plt.plot(ts, dist, 'b', label='smoothed')
	plt.show()
	
def plot_route_filtering(uri=schema.default_uri, shape=None, override=True):
	"""Used mainly for testing"""

	db = schema.connect(uri)
	conn = db.bind.raw_connection()
	
	if shape is not None:
		shapes = [shape]
	else:
		shapes = list(_fetch_shapes(conn, override))
	
	for shape in shapes:
		plot_shape_route_filtering(db, shape, override)


if __name__ == '__main__':
	import argh
        parser = argh.ArghParser()
        parser.add_commands([filter_routes, plot_route_filtering])
        parser.dispatch()

