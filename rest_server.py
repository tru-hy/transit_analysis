from StringIO import StringIO
import os
from collections import OrderedDict
import urlparse

import sqlalchemy as sqa

from ext.trusas_server import session_server, providers, serialize
from transit_analysis import recordtypes
import schema

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

@db_provider()
def coordinate_shapes(db, **kwargs):
	result = db.bind.execute("""
		select coordinate_shape.shape, coordinates
			from coordinate_shape
		where coordinate_shape.shape in
		(select distinct shape from transit_departure
		where routed_trace notnull)
		""")
	return resultoutput(result)

@db_provider()
def coordinate_shape(db, shape):
	result = db.bind.execute("""
		select * from coordinate_shape
		where shape=%s
		""", shape)
	
	row = result.fetchone()
	if not row: return None
	row = OrderedDict(row.items())
	return StringIO(serialize.dumps(row))

def nocols(tbl, *exclude):
	return [tbl.c[n] for (n) in tbl.c.keys() if n not in exclude]

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

def get_departure_traces(db, shape, route_variant=None, direction=None):
	dep = db.tables['transit_departure']
	tr = db.tables['routed_trace']
	
	
	cols = [dep, tr.c.time_at_distance_grid, tr.c.distance_bin_width]
	#+ nocols(tr, 'id', 'shape')
	query = sqa.select(cols).\
		where(dep.c.routed_trace==tr.c.id).\
		where(dep.c.shape==shape)
	
	if route_variant is not None:
		query = query.where(dep.c.route_variant==route_variant)
	if direction is not None:
		query = query.where(dep.c.direction==direction)
	
	return db.bind.execute(query)

@db_provider()
def departure_traces(db, **kwargs):
	return resultoutput(get_departure_traces(db, **kwargs))

import numpy as np
import scipy.stats

def axispercentile(values, percentiles):
	# TODO: WOW, how slow is this!
	results = np.empty((len(percentiles), values.shape[1]))
	for p, percentile in enumerate(percentiles):
		for i in range(values.shape[1]):
			results[p][i] = scipy.stats.scoreatpercentile(values[:,i],
				percentile*100)
	return results

class ShapeSession:
	def __init__(self, db, **kwargs):
		self.stop_span = 100.0

		self._query = kwargs
		self._result = list(get_departure_traces(db, **kwargs))
		self._binwidth = self._result[0].distance_bin_width
		self._timegrid = np.vstack(
			[r.time_at_distance_grid for r in self._result]
			)
		self._time_spent = np.diff(self._timegrid, axis=1)
		self._speed = 1.0/self._time_spent*self._binwidth*3.6
		self._timegrid = self._timegrid[:,1:]
		maxdist = self._timegrid.shape[1]*self._binwidth
		self._distgrid = np.arange(0, maxdist, self._binwidth)[:-1]
		
		self._stops = list(get_shape_stops(db, kwargs['shape']))

	
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
		percs = (
			('lowp', 0.05),
			('lowq', 0.25),
			('median', 0.5),
			('highq', 0.75),
			('highp', 0.95))
		results = axispercentile(self._time_spent, zip(*percs)[1])
		return {percs[i][0]: results[i] for i in range(len(percs))}
	
	def speed_stats(self):
		percs = (
			('lowp', 0.05),
			('lowq', 0.25),
			('median', 0.5),
			('highq', 0.75),
			('highp', 0.95))
		results = axispercentile(self._speed, zip(*percs)[1])
		return {percs[i][0]: results[i] for i in range(len(percs))}
	
	def stop_duration_stats(self):
		stats=dict(median=[], stop_share=[])
		for stop in self._stops:
			dist = stop.distance
			s = dist - self.stop_span/2.0
			e = dist + self.stop_span/2.0
			durs = self.span_durations(s, e)
			stats['median'].append(np.median(durs))
			valid = np.isfinite(durs)
			stats['stop_share'].append(np.sum(durs[valid] > 20)/float(np.sum(valid)))
		result = {k: np.array(v) for k, v in stats.iteritems()}
		return result
	
	def inter_stop_duration_stats(self):
		stats=dict(median=[])
		for i in range(len(self._stops)-1):
			s = self._stops[i].distance + self.stop_span/2.0
			e = self._stops[i+1].distance - self.stop_span/2.0
			stats['median'].append(np.median(self.span_durations(s, e)))
		result = {k: np.array(v) for k, v in stats.iteritems()}
		return result


	def distance_bin(self, distance):
		return min(max(0, int(distance/self._binwidth)),
			len(self._distgrid) - 1)
	
	def span_durations(self, start, end):
		s = self.distance_bin(float(start))
		e = self.distance_bin(float(end))
		return self._timegrid[:,e] - self._timegrid[:,s]

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
		RouteStatisticsProvider(db)
		]
	
	resources = session_server.ResourceServer(providers)
	my_static = os.path.join(os.path.dirname(__file__), 'ui')
	root = session_server.StaticUnderlayServer(my_static)
	root.resources = resources

	config = {
		'tools.gzip.on': True,
		'tools.gzip.mime_types': ['text/*', 'application/json']
		}
	cp.quickstart(root, config={'global': config})

if __name__ == '__main__':
	import argh
	argh.dispatch_command(main)

