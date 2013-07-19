from StringIO import StringIO
import os
from collections import OrderedDict

import sqlalchemy as sqa

from trusas_server import session_server, providers, serialize
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

@db_provider()
def departure_traces(db, shape, route_variant=None, direction=None):
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
	
	result = db.bind.execute(query)
	return resultoutput(result)

def main(uri=schema.default_uri):
	import cherrypy as cp
	
	db = schema.connect(uri)
	providers = [
		transit_routes(db),
		coordinate_shape(db),
		coordinate_shapes(db),
		departure_traces(db)
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
