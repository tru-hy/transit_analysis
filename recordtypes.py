from collections import namedtuple
import schema

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
	construct._fields = names
	return construct

for name, table in schema.metadata.tables.items():
	fields = []
	for cname, c in table.c.items():
		# TODO: Figure out if it's mandatory
		fields.append((cname, None))
	recordtype = defaultnamedtuple(name, fields)
	locals()[name] = recordtype


