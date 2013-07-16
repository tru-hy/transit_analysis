import sys

def csvmapper(field):
	if field is None:
		return "\N"
	if isinstance(field, basestring):
		return field
	if isinstance(field, unicode):
		return field.encode("utf-8")
	if hasattr(field, '__iter__'):
		return '{'+','.join(map(csvmapper, field)) + '}'
	
	return unicode(field).encode("utf-8")


class TraceTracker:
	def __init__(self, on_departure):
		self.ongoing = {}
		self._reported = {}
		self.cb = on_departure
	
	def _on_departure(self, src, *args):
		dep = args[0]
		if dep in self._reported:
			print >>sys.stderr, "Duplicate trace for %s"%dep
			return
		self._reported[dep] = True
		self.cb(src, *args)
	
	def __call__(self, departure, measurement):
		src = measurement.source
		if not src:
			return
		if src not in self.ongoing:
			self.ongoing[src] = [departure, measurement.time,
						measurement.time]
			return
		record = self.ongoing[src]
		if record[0] != departure:
			self._on_departure(src, *record)
			self.ongoing[src] = [departure, measurement.time,
						measurement.time]
			return
		record[-1] = measurement.time



	def finalize(self):
		for src, record in self.ongoing.iteritems():
			self._on_departure(src, *record)

