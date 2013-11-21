#!/usr/bin/env python2

from StringIO import StringIO
import os
from collections import OrderedDict
import urlparse

import sqlalchemy as sqa
import networkx as nx
import numpy as np
import scipy.stats


from ext.trusas_server import session_server, providers, serialize
import recordtypes
import schema
import config

def _to_nulls(lst):
	# Hack to convert NaNs and Infs to JSON nulls
	if not isinstance(lst, list):
		return lst
	for i in range(len(lst)):
		if isinstance(lst[i], list):
			_to_nulls(lst[i])
			continue
		if lst[i] != lst[i]:
			lst[i] = None
	return lst
		

def serialize_row(row):
	output = OrderedDict()
	for n, v in row.items():
		output[n] = _to_nulls(v)
	return output

def resultoutput(result):
	output = StringIO()

	serialize.dump(
		[serialize_row(row) for row in result],
		output)
	output.seek(0)
	return output

def db_provider(default_path=None, mime_args=None):
	mime_type = "application/json"
		
	def generate_class(handler):
		if default_path is None:
			path = handler.__name__ + ".json"
		else:
			path = default_path

		if mime_args is None:
			mime_str = mime_type + "; trusas_type="+handler.__name__
		else:
			mime_str = mime_type + "; %s"%(mime_args,)


		class DbProviderWrapper(providers.PathProvider):
			def __init__(self, db, mypath=path):
				self.db = db
				super(DbProviderWrapper, self).__init__(
					mypath, mime_str)

			def handle(self, **kwargs):
				return handler(self.db, **kwargs)

		return DbProviderWrapper

	return generate_class

@db_provider()
def transit_routes(db, **kwargs):
	cols = "route_name, route_variant, direction, shape"

	result = db.bind.execute("""
		select count(*) as departures,
			%(cols)s,
			concat(%(cols)s) as id
		from transit_departure
		where routed_trace notnull
		group by %(cols)s
		order by departures desc
		"""%dict(cols=cols))
	return resultoutput(result)

def get_active_coordinate_shapes(db):
	return db.bind.execute("""
		select coordinate_shape.shape, coordinates, node_ids, distances
			from coordinate_shape
		where coordinate_shape.shape in
		(select distinct shape from transit_departure
		where routed_trace notnull)
		""")

@db_provider()
def coordinate_shapes(db, **kwargs):
	result = get_active_coordinate_shapes(db)
	return resultoutput(result)

def get_coordinate_shape(db, shape):
	result = db.bind.execute("""
		select * from coordinate_shape
		where shape=%s
		""", shape)
	
	result = dict(result.fetchone())
	return result

@db_provider()
def coordinate_shape(db, shape):
	row = get_coordinate_shape(db, shape)
	if not row: return None
	row = OrderedDict(row.items())
	return StringIO(serialize.dumps(row))

def route_graph(db):
	shapes = get_active_coordinate_shapes(db)
	shapes = list(shapes)
	graph = nx.DiGraph()
	
	positions = {}
	for shape in shapes:
		nodes = shape['node_ids'][1:-1]
		coords = shape['coordinates'][1:-1]
		distances = shape['distances'][1:-1]
		for i in range(len(nodes)-1):
			positions[nodes[i]] = coords[i]
			s, e = nodes[i], nodes[i+1]
			graph.add_edge(nodes[i], nodes[i+1])
			edge = graph[s][e]
			if "shapes" not in edge:
				edge['shapes'] = list()
			edge['shapes'].append(shape.shape)
			edge['distance'] = distances[i+1] - distances[i]
		positions[nodes[-1]] = coords[-1]
	
	return graph, positions, shapes


@db_provider()
def route_graph_edges(db, **kwargs):
	graph, positions, shapes = route_graph(db)
	out = {
		'edges': graph.edges(data=True),
		'nodes': positions
		}
	return serialize.dumps(out)


def nocols(tbl, *exclude):
	return [tbl.c[n] for (n) in tbl.c.keys() if n not in exclude]

WEEKDAY_NUMBERS = {
	'mon': 1,
	'tue': 2,
	'wed': 3,
	'thu': 4,
	'fri': 5,
	'sat': 6,
	'sun': 0
	}

def get_shape_stops(db, shape):
	q = """
	with ss as (
	select
	 unnest(stop_ids) as stop_id,
	 unnest(distances) as distance
	from transit_shape_stop
	where shape_id=%(shape)s
	)
	select ss.*, stop_name, latitude, longitude
	from ss
	join transit_stop as s on s.stop_id=ss.stop_id
	order by distance
	"""
	return db.bind.execute(q, shape=shape)

def get_departure_traces(db, shape, route_variant=None, direction=None,
			start_date=None, end_date=None, weekdays=None):
	dep = db.tables['transit_departure']
	tr = db.tables['routed_trace']
	
	
	depdate = sqa.func.date(dep.c.departure_time.label('departure_date'))
	cols = [dep, tr.c.time_at_distance_grid, tr.c.distance_bin_width,
		depdate]
	#+ nocols(tr, 'id', 'shape')
	query = sqa.select(cols).\
		where(dep.c.routed_trace==tr.c.id).\
		where(dep.c.shape==shape)
	
	rangequery = sqa.select([
		sqa.func.min(dep.c.departure_time).label('mindate'),
		sqa.func.max(dep.c.departure_time).label('maxdate')
		]).\
		where(dep.c.routed_trace==tr.c.id).\
		where(dep.c.shape==shape)
	
	if route_variant is not None:
		query = query.where(dep.c.route_variant==route_variant)
		rangequery = rangequery.where(dep.c.route_variant==route_variant)
	if direction is not None:
		query = query.where(dep.c.direction==direction)
		rangequery = rangequery.where(dep.c.direction==direction)
	if start_date is not None:
		query = query.where(depdate >= start_date)
	if end_date is not None:
		query = query.where(depdate <= end_date)
	if weekdays:
		numbered = tuple(WEEKDAY_NUMBERS[d] for d in weekdays)
		q = "extract('dow' from departure_time) in :dow"
		query = query.where(q).params(dow=numbered)
	
	range_result = dict(db.bind.execute(rangequery).fetchone())

	return db.bind.execute(query), range_result

def get_node_path_traces(db, route_nodes, start_date=None, end_date=None, weekdays=None):
	graph, positions, shapes = route_graph(db)
	
	mindist = float("inf")
	minnodes = None
	mindistances = None
	candidates = []
	for shape in shapes:
		prev = -1
		idx = []
		try:
			for node in route_nodes:
				next = shape.node_ids[prev+1:].index(node)
				idx.append(next+prev+1)
				prev = idx[-1]
		except ValueError:
			continue
		
		
		sdist = shape.distances[idx[0]]
		edist = shape.distances[idx[-1]]
		dist = edist - sdist

		nodes = shape.node_ids[idx[0]:idx[-1]+1]
		distances = shape.distances[idx[0]:idx[-1]+1]
		candidates.append((shape, nodes, distances))
		if dist < mindist:
			mindist = dist
			minnodes = nodes
			mindistances = distances
	
	active_shapes = []
	for shape, nodes, distances in candidates:
		if nodes != minnodes:
			continue
		active_shapes.append((shape, (distances[0], distances[-1])))
		
	
	distances = mindistances
	
	datefilter = ""
	qargs = {}
	if start_date:
		datefilter += " and date(transit_departure.departure_time) >= %(start_date)s"
		qargs['start_date'] = start_date
	if end_date:
		datefilter += " and date(transit_departure.departure_time) <= %(end_date)s"
		qargs['end_date'] = end_date
	if weekdays:
		datefilter += " and extract(dow from transit_departure.departure_time) in %(weekdays)s"
		qargs['weekdays'] = tuple(WEEKDAY_NUMBERS[d] for d in weekdays)

	path = minnodes
	result = []
	for shape, (startd, endd) in active_shapes:
		mqargs = dict(qargs)
		mqargs['startd'] = startd
		mqargs['endd'] = endd
		mqargs['shape'] = shape['shape']
		query = """
		select id, reference_time, distance_bin_width,
			time_at_distance_grid[trunc(%%(startd)s/distance_bin_width):trunc(%%(endd)s/distance_bin_width)] as time_at_distance_grid,
			date(transit_departure.departure_time) as departure_date,
			transit_departure.*
		from routed_trace
		join transit_departure on transit_departure.routed_trace=id
		where transit_departure.shape=%%(shape)s %s
		"""%(datefilter,)
		data = db.bind.execute(query, **mqargs)
		result.extend(map(dict, data))
	
	# Make sure the grids are of equal size. This may have one-bin
	# difference due to rounding
	minlength = min((len(d['time_at_distance_grid']) for d in result))
	for drive in result:
		drive['time_at_distance_grid'] = drive['time_at_distance_grid'][:minlength]
	
	distances = np.array(distances)-distances[0]
	fake_shape = {
		'coordinates': [positions[n] for n in path],
		'distances': list(distances)
		}
	
	date_range = """
		select min(departure_time) as mindate, max(departure_time) as maxdate
		from transit_departure
		where routed_trace notnull
		and shape in %(shape_ids)s"""
	
	
	
	shape_ids = [s[0].shape for s in active_shapes]

	shape_ids.append('adsfasfaf')
	shape_ids = tuple(shape_ids)
	date_range = db.bind.execute(date_range, shape_ids=shape_ids)
	date_range = dict(date_range.fetchone())

	return result, fake_shape, date_range

@db_provider()
def departure_traces(db, **kwargs):
	return resultoutput(get_departure_traces(db, **kwargs)[0])


def axispercentile(values, percentiles):
	if values.size == 0:
		return np.empty((len(percentiles), 0))
	

	# TODO: WOW, how slow is this!
	results = np.empty((len(percentiles), values.shape[1]))
	for p, percentile in enumerate(percentiles):
		for i in range(values.shape[1]):
			results[p][i] = scipy.stats.scoreatpercentile(values[:,i],
				percentile*100)
	return results

percs = (
		('lowp', 0.05),
		('lowq', 0.25),
		('median', 0.5),
		('highq', 0.75),
		('highp', 0.95))

class ShapeSession:
	def __populate_data(self):
		parsed_query = dict(self._query)

		if 'weekdays' in parsed_query:
			parsed_query['weekdays'] = parsed_query['weekdays'].split(',')

		if "route_nodes" in self._query:
			nodes = parsed_query['route_nodes'].split(',')
			parsed_query['route_nodes'] = nodes
			self._result, self._shape, date_range = get_node_path_traces(self._db, **parsed_query)
			self.date_range = lambda: date_range
			self._stops = []
		else:
			self._result, daterange = get_departure_traces(self._db, **parsed_query)
			self._result = list(self._result)
			self.date_range = lambda: daterange
			self._stops = list(get_shape_stops(self._db, self._query['shape']))
			self._shape = get_coordinate_shape(self._db, self._query['shape'])

	def __init__(self, db, **kwargs):
		self.stop_span = 100.0
		
		self._db = db
		self._query = kwargs
		self.__populate_data()
		
		self._binwidth = self._result[0]['distance_bin_width']
		self._timegrid = np.vstack(
			[r['time_at_distance_grid'] for r in self._result]
			)
		self._time_spent = np.diff(self._timegrid, axis=1)
		self._speed = 1.0/self._time_spent*self._binwidth*3.6
		self._timegrid = self._timegrid[:,1:]
		maxdist = self._timegrid.shape[1]*self._binwidth
		self._distgrid = np.arange(0, maxdist, self._binwidth)[:-1]
		

	
	def _mymethods(self):
		def ismethod(d):
			return (not d.startswith('_')
				and callable(getattr(self, d)))
		return {d: [] for d in dir(self) if ismethod(d)}
	
	def __call__(self, *path, **kwargs):
		return serialize.result(self._handle(*path, **kwargs))

	def _handle(self, *path, **kwargs):
		if len(path) == 0:
			return dict(query=self._query, methods=self._mymethods())
		if len(path) == 1 and "&" not in path[0]:
			if path[0].startswith('_'):
				raise AttributeError
			if not hasattr(self, path[0]):
				raise AttributeError
			
			return getattr(self, path[0])(**kwargs)

		result = {}
		for query in path:
			parts = query.split('&', 1)
			method = parts[0]
			if len(parts) == 1:
				kwargs = {}
			else:
				kwargs = {k: v[0]
					for k, v in urlparse.parse_qs(parts[1]).items()}
			result[method] = self._handle(method, **kwargs)
		return result
	
	def departures(self):
		out = []
		fields = recordtypes.transit_departure._fields
		for r in self._result:
			obj = {k: r[k] for k in fields if k in r}
			out.append(obj)
		return out
			
	def stops(self):
		def addindex(rows):
			for i, row in enumerate(rows):
				row = OrderedDict(row.items())
				row['index'] = i
				yield row

		return list(addindex(self._stops))
	
	def distance_grid(self):
		return self._distgrid
	
	def time_spent_stats(self):
		results = axispercentile(self._time_spent, zip(*percs)[1])
		return {percs[i][0]: results[i] for i in range(len(percs))}
	
	def speed_stats(self):
		results = axispercentile(self._speed, zip(*percs)[1])
		return {percs[i][0]: results[i] for i in range(len(percs))}
	
	def stop_duration_stats(self):
		alldurs = []
		stop_share = []
		stats=dict(median=[], stop_share=[])
		for stop in self._stops:
			dist = stop.distance
			s = dist - self.stop_span/2.0
			e = dist + self.stop_span/2.0
			durs = self.span_durations(s, e)
			alldurs.append(durs)
			valid = np.isfinite(durs)
			stop_share.append(np.sum(durs[valid] > 20)/float(np.sum(valid)))
		#result = {k: np.array(v) for k, v in stats.iteritems()}
		results = axispercentile(np.array(alldurs).T, zip(*percs)[1])
		results = {percs[i][0]: results[i] for i in range(len(percs))}
		results['stop_share'] = np.array(stop_share)
		return results
	
	def inter_stop_duration_stats(self):
		durs = []
		for i in range(len(self._stops)-1):
			s = self._stops[i].distance + self.stop_span/2.0
			e = self._stops[i+1].distance - self.stop_span/2.0
			durs.append(self.span_durations(s, e))
		results = axispercentile(np.array(durs).T, zip(*percs)[1])
		return {percs[i][0]: results[i] for i in range(len(percs))}
		
	def distance_bin(self, distance):
		return min(max(0, int(distance/self._binwidth)),
			len(self._distgrid) - 1)
	
	def span_durations(self, start, end):
		s = self.distance_bin(float(start))
		e = self.distance_bin(float(end))
		return self._timegrid[:,e] - self._timegrid[:,s]
	
	def coordinate_shape(self):
		return self._shape

class RouteStatisticsProvider:
	def __init__(self, db, mypath="route_statistics"):
		self.mypath = mypath
		self.sessions = {}
		self.db = db
	
	def provides(self):
		return {self.mypath: "application/json"}

	def __call__(self, *path, **kwargs):
		if len(path) == 0:
			return None
		if path[0] != self.mypath:
			return None
		if len(path) >= 2:
			return self._load_session(path[1])(*path[2:], **kwargs)
		
		return self._new_session(**kwargs)
	
	def _load_session(self, session_key):
		if session_key in self.sessions:
			return self.sessions[session_key]
		kwargs = {k: v[0] for k, v in urlparse.parse_qs(session_key).items()}
		self._new_session(**kwargs)
		return self.sessions[session_key]

	def _new_session(self, **kwargs):
		session_key = "&".join(("%s=%s"%(k, v) for k, v in kwargs.items()))
		if session_key not in self.sessions:
			self.sessions[session_key] = ShapeSession(self.db, **kwargs)
		
		result = self.sessions[session_key]._handle()
		result['session_key'] = session_key
		return serialize.result(result)
	
		
	
def main(uri=schema.default_uri):
	import cherrypy as cp
	
	db = schema.connect(uri)
	providers = [
		transit_routes(db),
		coordinate_shape(db),
		coordinate_shapes(db),
		departure_traces(db),
		RouteStatisticsProvider(db),
		route_graph_edges(db)
		]
	
	resources = session_server.ResourceServer(providers)
	my_static = os.path.join(os.path.dirname(__file__), 'ui')
	root = session_server.StaticUnderlayServer(my_static)
	root.resources = resources

	cpconfig = {
		'server.socket_host': config.server_host,
		'server.socket_port': config.server_port,
		'tools.gzip.on': True,
		'tools.gzip.mime_types': ['text/*', 'application/json']
		}
	cp.quickstart(root, config={'global': cpconfig})

if __name__ == '__main__':
	import argh
	argh.dispatch_command(main)

