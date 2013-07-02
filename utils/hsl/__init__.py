from collections import namedtuple

def departure_key(route_variant, direction, departure_time):
	return "/".join(map(str, (
		route_variant,
		direction,
		departure_time)))


def departure_record_key(dep):
	return departure_key(dep.route_variant,
		dep.direction, dep.departure_time)


departure_type = namedtuple("departure", (
	'route_name', 'route_variant', 'direction',
	'departure_time', 'shape', 'attributes'
		))

def departure_tuple_to_row(dep):
	values = dep._asdict()
	values['departure_id'] = departure_key(dep)
	return values
