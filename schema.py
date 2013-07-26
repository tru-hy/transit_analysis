from sqlalchemy import *
from sqlalchemy.dialects.postgresql import HSTORE, ARRAY

from transit_analysis import config
metadata = MetaData()

Table("coordinate_trace", metadata,
	Column('id', String(255), primary_key=True),
	Column('source', String(255)),
	Column('start_time', DateTime),
	Column('end_time', DateTime)
	)

Table("routed_trace", metadata,
	Column('id', Integer, Sequence("routed_trace_id_seq"),
		primary_key=True),
	Column('reference_time', DateTime),
	Column('shape', String(255)),
	Column('timestamp', ARRAY(Float)),
	Column('route_distance', ARRAY(Float)),
	Column('route_speed', ARRAY(Float)),
	Column('time_at_distance_grid', ARRAY(Float)),
	Column('distance_bin_width', Float)
	)

Table("coordinate_measurement", metadata,
	Column('source', String(255), index=True),
	Column('time', DateTime, index=True),
	Index('source_time_idx', 'source', 'time'),
	Column('latitude', Float),
	Column('longitude', Float),
	Column('altitude', Float),
	Column('bearing', Float),
	Column('velocity', Float),
	)

Table("transit_departure", metadata,
	Column('departure_id', String(255), primary_key=True),
	# TODO: The information here is quite HSL specific and
	#	could be in another table
	Column('route_name', String(255)),
	Column('route_variant', String(255)),
	Column('direction', String(255)),
	Column('departure_time', DateTime),
	Column('shape', String(255)),
	Index('departure_shape_idx', 'shape'),
	Column('trace', String(255)),
	Column('routed_trace', Integer,
		ForeignKey("routed_trace.id"),
		nullable=True),
	Column('attributes', HSTORE),
	Column('schedule_id', String(255))
	)

Table("transit_stop", metadata,
	Column('stop_id', String(255), primary_key=True),
	Column('stop_name', String(255)),
	Column('latitude', Float),
	Column('longitude', Float)
	)

Table("transit_schedule_shape", metadata,
	Column('schedule_id', String(255), primary_key=True),
	Column('shape_id', String(255), primary_key=True),
	)

Table("transit_schedule_stop", metadata,
	Column('schedule_id', String(255), primary_key=True),
	Column('stop_id', String(255), primary_key=True),
	Column('arrival', Float),
	Column('departure', Float)
	)


Table("coordinate_shape", metadata,
	Column('shape', String(255), primary_key=True),
	Column('coordinates', ARRAY(Float, dimensions=2)),
	Column('distances', ARRAY(Float))
	)


Table("transit_shape_stop", metadata,
	Column('shape_id', String(255), primary_key=True),
	Column('distances', ARRAY(Float)),
	Column('stop_ids', ARRAY(String(255))),
	Column('sequence', ARRAY(Integer)),
	)

default_uri=config.db_connection_uri

def connect(uri=default_uri):
	engine = create_engine(uri)
	metadata.bind = engine
	return metadata

def create_user(user='transit', password='transit',
		admin_uri="postgres://postgres@/postgres"):
	engine = create_engine(admin_uri)
	conn = engine.connect()
	# Have to close the transaction before creating
	conn.execute("commit") 
	
	conn.execute("create user %s with password %%s"%user, password)

def create_database(owner='transit', database='transit',
		admin_uri="postgres://postgres@/postgres"):
	conn = create_engine(admin_uri).connect()
	conn.execute("commit")
	
	conn.execute("create database %s with owner %s"%(database, owner))
	conn.close()

	# SO ANNYOING!!!
	new_uri = admin_uri.rsplit("/", 1)[0] + "/" + database
	conn = create_engine(new_uri).connect()
	conn.execute("CREATE EXTENSION IF NOT EXISTS hstore")

def drop_database(owner='transit', database='transit',
		admin_uri="postgres://postgres@/postgres"):
	conn = create_engine(admin_uri).connect()
	conn.execute("commit")
	
	conn.execute("drop database %s"%(database,))
	conn.close()

def initialize_schema(uri=default_uri):
	metadata = connect(uri)
	conn = metadata.bind.connect()
	metadata.create_all()

def _propagate_kwargs(f, params):
	names = f.func_code.co_varnames[:f.func_code.co_argcount]
	kwargs = {n: params[n] for n in names if params.get(n) is not None}
	return f(**kwargs)

def reset_db(uri=None, owner=None, database=None, admin_uri=None):
	# Hackidi hack
	kwargs = {k: v for k, v in locals().iteritems() if v is not None}

	_propagate_kwargs(drop_database, kwargs)
	_propagate_kwargs(create_database, kwargs)
	_propagate_kwargs(initialize_schema, kwargs)

if __name__ == '__main__':
        import argh
        parser = argh.ArghParser()
        parser.add_commands([create_database, create_user, initialize_schema, reset_db])
        parser.dispatch()
	
