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

coord_proj = pyproj.Proj(init="EPSG:3857")

#shape = "1007B_20080707_2"
#shape = "1008_20121001_2"
#shape = "1001_20110815_2"
#shape = "1007A_20080707_1"
#shape = "1006_20121001_1"
shape = "1009_20120813_1"

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
	return (slice(changes[i], changes[i+1]) for i in range(0, len(changes)-1, 2))

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

def map_departure_to_route(ts, cart, shape_string, new_dt=1.0):
	min_werr = 0.1
	
	route_mapping = np.array(list(project_to_linestring(cart, shape_string)))
	route_distance = route_mapping[:,0]
	on_string = route_mapping[:,1:]
	err = np.sqrt(np.sum((on_string - cart)**2, axis=1))

	invalid = out_of_route(err)
	valid_blocks = find_valid_blocks(invalid)
	blocks = []
	for block in valid_blocks:
		werr = err[block].copy()
		werr[werr < min_werr] = min_werr
		w = np.sqrt(100*1.0/werr)
		n = block.stop - block.start
		# TODO: Doing the blocking without interpolating would
		#	be a lot faster
		# TODO: Use better smoothing, eg L1 splines. This tends
		#	to cause oscillations
		try:
			splinefit = scipy.interpolate.UnivariateSpline(
				ts[block], route_distance[block], w=w, s=(n-np.sqrt(2*n)))
		except:
			print >>sys.stderr, "Spline fitting failed, skipping block"
			continue

		#plt.plot(ts[block], route_distance[block])
		#plt.plot(ts[block][1:], np.diff(route_distance[block])/np.diff(ts[block]))
		#plt.plot(ts[block], splinefit(ts[block], 1))
		#plt.show()
		
		mono = find_approx_monotonic_blocks(splinefit(ts[block], 1))
		mono = ((slice(m.start+block.start, m.stop+block.start), splinefit)
			for m in mono)
		blocks.extend(mono)
	
	if len(blocks) == 0:
		raise RouteException("No valid blocks found")

	# Get block of which start time is closest to the
	# departure time (assuming ts is relative to the departure time)
	block = blocks[np.argmin([ts[b[0].start] for b in blocks])]
	block, splinefit = block
	
	new_ts = np.arange(ts[block.start], ts[block.stop-1], new_dt)
	fitted_route_distance = splinefit(new_ts)
	fitted_route_distance = scipy.ndimage.gaussian_filter1d(fitted_route_distance, 3.0/new_dt)
	route_speed = np.diff(fitted_route_distance)
	fitted_route_distance = fitted_route_distance[1:]
	block = slice(block.start+1, block.stop)
	if np.mean(route_speed) < 0:
		# The route should be driven in the presented direction
		raise RouteException("Wrong driving direction")

	new_ts = new_ts[1:]
	return new_ts, fitted_route_distance, route_speed

def filter_shape_routes(db, shape):
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
			""", (departure.start_time,
				departure.source,
				departure.start_time,
				departure.end_time))
		
		coordinates = coord_cur.fetchall()
		if len(coordinates) < 10: continue
		ts, lat, lon = np.array(coordinates).T
		cart = np.array(zip(*coord_proj(lon, lat)))
		try:
			yield departure, map_departure_to_route(ts, cart, shape_string)
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


def filter_routes(uri=schema.default_uri, shape=None):
	db = schema.connect(uri)
	conn = db.bind.raw_connection()
	
	#shape = "1009_20120813_1"
	
	if shape is not None:
		shapes = [shape]
	else:
		cur = conn.cursor()
		cur.execute("""
			select distinct shape
			from transit_departure
			where trace notnull
			""")
		shapes = [r[0] for r in cur]
	
	routed_tbl = db.tables['routed_trace']
	departure_tbl = db.tables['transit_departure']
	n_shapes = len(shapes)
	for i, shape in enumerate(shapes):
		print >>sys.stderr, "Processing %i/%i shapes"%(i+1, n_shapes)
		mappings = filter_shape_routes(db, shape)
		for mapping in mappings:
			insert_routed_trace(routed_tbl, departure_tbl, mapping)


if __name__ == '__main__':
	import argh
        parser = argh.ArghParser()
        parser.add_commands([filter_routes])
        parser.dispatch()


"""
	route_distance = map(shape_string.project, points)
	on_string = map(shape_string.interpolate, route_distance)
	err = np.array([on_string[i].distance(points[i]) for i in range(len(points))])

	invalid = out_of_route(err)
	#weights = 1.0/(err+1.0)

	route_distance = np.array(route_distance)
	ts = np.array(ts)

	werr = err.copy()
	werr[werr < 1] = 1
	w = 1.0/werr
	splinefit = scipy.interpolate.UnivariateSpline(ts, route_distance, w=w, s=len(ts))

	#plt.plot(ts[~invalid], route_distance[~invalid], label='orig')
	#plt.plot(ts[~invalid], filtdist[~invalid], label='filtered')
	plt.subplot(2,1,1)
	plt.plot(ts[1:], np.diff(route_distance), label='orig')
	fitted = splinefit(ts)
	fitted[invalid] = np.nan
	plt.plot(ts, splinefit(ts, 1))
	#plt.plot(ts[valid], lowpass, label='filtered')
	#plt.subplot(2,1,2)
	#plt.plot(ts, err)
	plt.subplot(2,1,2)
	plt.plot(shape_cart[0], shape_cart[1])
	plt.plot(cart[:,0][~invalid], cart[:,1][~invalid], '.', color='black', alpha=0.3)
	plt.plot(cart[:,0][invalid], cart[:,1][invalid], '.', color='red', alpha=0.3)
	plt.legend()
	plt.show()
	
	#plt.subplot(1,2,1)
	#plt.plot(shape_cart[0], shape_cart[1], '-o')
	#plt.plot(cart[:,0], cart[:,1], '.', color='black', alpha=0.1)
	#plt.subplot(1,2,2)
	#plt.plot(ts, dists, '.')
	#plt.show()

plt.show()
"""
