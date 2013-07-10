from StringIO import StringIO
import os
from collections import OrderedDict

import simplejson

from trusas_server import session_server, providers
import schema

def resultoutput(result):
	output = StringIO()
	simplejson.dump(
		[OrderedDict(row.items()) for row in result],
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
		select coordinates from coordinate_shape
		where shape=%s
		""", shape)
	
	row = result.fetchone()
	if not row: return None
	row = OrderedDict(row.items())
	return StringIO(simplejson.dumps(row))

def main(uri=schema.default_uri):
	import cherrypy as cp
	
	db = schema.connect(uri)
	providers = [
		transit_routes(db),
		coordinate_shape(db),
		coordinate_shapes(db),
		]
	
	resources = session_server.ResourceServer(providers)
	my_static = os.path.join(os.path.dirname(__file__), 'ui')
	root = session_server.StaticUnderlayServer(my_static)
	root.resources = resources
	cp.quickstart(root)

if __name__ == '__main__':
	import argh
	argh.dispatch_command(main)
