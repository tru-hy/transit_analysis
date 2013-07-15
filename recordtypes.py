from collections import namedtuple

def defaultnamedtuple(name, fields, *args, **kwargs):
	names = []
	defaults = {}
	for field in fields:
		if isinstance(field, basestring):
			names.append(field)
		else:
			names.append(field[0])
			defaults[field[0]] = field[1]
	impl = namedtuple(name, names, *args, **kwargs)
	def construct(*args, **kwargs):
		for name in names[len(args):]:
			if name in kwargs: continue
			kwargs[name] = defaults[name]
		return impl(*args, **kwargs)
	return construct
	

transit_departure = defaultnamedtuple("transit_departure", (
	'departure_id',
	('route_name', None),
	('route_variant', None),
	('direction', None),
	('departure_time', None),
	('shape', None),
	('trace', None),
	('routed_trace', None),
	('attributes', None)
	))


coordinate_measurement = defaultnamedtuple("coordinate_measurement", (
	'source',
	'time',
	'latitude',
	'longitude',
	('altitude', None),
	('bearing', None),
	('velocity', None)
	))

coordinate_trace = defaultnamedtuple("coordinate_trace", (
	'id',
	'source',
	'start_time',
	'end_time'
	))
