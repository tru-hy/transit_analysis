# Don't edit this file. Create a config_local.py
# to the project root and it will override any values
# here.

server_port = 8080
server_host = "127.0.0.1"

coordinate_projection = "epsg:3067"
db_connection_uri = "postgres://transit:transit@localhost/transit"

max_drives_per_session = 3000
max_cached_sessions = 10

try:
	import config_local
	for k in dir(config_local):
		if k.startswith('_'): continue
		locals()[k] = getattr(config_local, k)
except ImportError:
	pass
