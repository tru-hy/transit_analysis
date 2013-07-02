# Transit Analysis

A work in progress software system for analyzing transit data.
In very early development.


## Getting started

The system currently consists mostly of the database
schema and utilities to dump some data in to the database.
The scripts currently understand only [Helsinki's public
transportation data](http://developer.reittiopas.fi/pages/en/home.php).

Requires Python 2.x (developed on 2.7), modern version of Postgresql
(developed on 9.2) and a bunch of more or less obscure Python
packages.

### Initialize database

To initialize the database, run the following:

	python2 schema.py create-user --user=transit --password=transit \
	    --admin-uri=postgres://postgres@/postgres
	python2 schema.py create-database --database=transit \
	    --owner=transit --admin-uri=postgres://postgres@/postgres
	python2 schema.py initialize-schema --uri=postgres://transit:transit@/transit

(Replace the connection uri's etc with something to your liking. If you use the
default database, user and password, (transit, transit, transit), you can skip
the uri parameter in the scripts.)

### Import data

The import scripts currently work only on Helsinki public transportation
data due to some schema mismatch between GTFS and the live data. The
data is available at 

Import gtfs data:

	python2 utils/hsl/load_gtfs.py load-hsl-gtfs path_to_gtfs_directory \
	    --uri=postgres://transit:transit@/transit

Load the live data and map it to departures:

	cat path/to/hsllive.txt | \
	python2 utils/hsl/load_measurements.py load-measurements \
	    --uri=postgres://transit:transit@/transit | \
	psql -U transit -c "copy coordinate_measurement from stdin"

The live dumps tend to have a lot (tens to hundreds of millions) of records,
so for quick you may want to use only a subset by replacing the `cat`
eg by `tail -n 10000000` to get ten million newest rows. Just stopping the
script by eg `ctrl-c` doesn't work as the psql transaction gets rolled back.

**NOTE!** The data must be currently imported in this order.

**NOTE!** The scripts/db schema currently don't check for duplicates mostly for
performance and flexibility in testing. So to avoid duplicates, clear the database
(or relevant tables) between importing.
