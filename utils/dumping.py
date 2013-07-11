def csvmapper(field):
	if field is None:
		return "\N"
	return str(field)


class TraceTracker:
	def __init__(self, on_departure):
		self.ongoing = {}
		self.on_departure = on_departure
	
	def __call__(self, departure, measurement):
		src = measurement.source
		if src not in self.ongoing:
			self.ongoing[src] = [departure, measurement.time,
						measurement.time]
			return
		record = self.ongoing[src]
		if record[0] != departure:
			self.on_departure(src, *record)
			self.ongoing[src] = [departure, measurement.time,
						measurement.time]
			return
		record[-1] = measurement.time



	def finalize(self):
		for src, record in self.ongoing.iteritems():
			self.on_departure(src, *record)
