import urllib2
import urlparse
from base64 import b64encode
import sys
import time

vehicle_poll_template = """<?xml version="1.0" encoding="UTF-8"?>
<Siri xmlns="http://www.siri.org.uk/siri" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" version="1.3" xsi:schemaLocation="http://www.kizoom.com/standards/siri/schema/1.3/siri.xsd"><ServiceRequest><RequestorRef>%(app_id)s</RequestorRef><VehicleMonitoringRequest version="1.3"><VehicleMonitoringRef>VEHICLES_ALL</VehicleMonitoringRef></VehicleMonitoringRequest></ServiceRequest></Siri>"""

	
def get_request(url):
	parsed = urlparse.urlparse(url)
	credentials = None
	netloc = parsed.hostname
	if parsed.port is not None:
		netloc += ":%i"%parsed.port
	
	if parsed.username is not None and parsed.password is not None:
		credentials = "%s:%s"%(parsed.username, parsed.password)
		credentials = b64encode(credentials)

	
	parsed = list(parsed)
	parsed[1] = netloc
	url = urlparse.urlunparse(parsed)
	req = urllib2.Request(url)
	if credentials:
		req.add_header('Authorization', "Basic %s"%credentials)

	return req

def fileredirect(src, dst, bufsize=1024):
	while True:
		buf = src.read(bufsize)
		if not buf:
			break
		dst.write(buf)
		dst.flush()

class NicerPollwait:
	def __init__(self, interval=1.0, max_interval=8.0):
		self.interval = interval
		self.max_interval = max_interval
		self.retry_time = interval
		self.prev_time = time.time()
	
	def success(self):
		self.retry_time = self.interval
		now = time.time()
		time_left = self.prev_time + self.interval - now
		if time_left > 0:
			time.sleep(time_left)
		self.prev_time = time.time()
	
	def failure(self):
		time.sleep(self.retry_time)
		self.retry_time *= 2
		self.retry_time = max(self.retry_time, self.max_interval)
		
def main(url, app_id):
	request = get_request(url)
	query = vehicle_poll_template%dict(app_id=app_id)
	request.add_data(query)
	
	poller = NicerPollwait()
	output = sys.stdout
	
	while True:
		try:
			result = urllib2.urlopen(request)
			fileredirect(result, output)
			output.write('\n')
			pass
		except Exception, e:
			print >>sys.stderr, "Polling failed:", e
			poller.failure()
			continue
		poller.success()

if __name__ == '__main__':
	import argh
	argh.dispatch_command(main)
