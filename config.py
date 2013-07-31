server_port = 8080

coordinate_projection = "epsg:3067"
db_connection_uri = "postgres://transit:transit@localhost/transit"

import config_local
for k in dir(config_local):
	if k.startswith('_'): continue
	locals()[k] = getattr(config_local, k)

