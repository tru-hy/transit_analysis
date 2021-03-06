# Transit Analysis

A software system for visualizing and analyzing transit data.
Under development.

## Getting started

### Installation

Get the sources using (modern version of) git (including submodules):

    git clone --recursive https://github.com/tru-hy/transit_analysis.git
    cd transit_analysis

#### Quick

For debian-style distribitions, you can run an installer script that
tries to set the stuff up using default values. You'll need to have
sudo to run it (it'll ask the password as it goes):
   
    ./install.debian.bash



#### Slow

TODO. See what's done in [install.debian.bash](install.debian.bash).

### Import data

*NOTE* The data importation is far from streamlined at the moment.
All this should be done with one or two simple commands.

Due to Python's challenged (to be polite) import system, currently
the parent of the `transit_analysis` directory has to be in PYTHONPATH
or somehow else findable by the import mechanism by name `transit_analysis`
to run the import scripts. You need to also have activated the virtualenv
by running `source .virtualenv/bin/activate`.

#### GTFS

Currently the only supported data format for the transit "metadata" is GTFS.
All GTFS data that don't directly map with live data can be loaded with:

    cd utils/gtfs
    PYTHONPATH=../../../ ./load_common.bash <gtfs directory>

Where `<gtfs directory>` is directory containing the gtfs .txt files.

The system also (currently) requires version of shapes.txt which has ids specifying
the (OSM) nodes corresponding to the shape points. This can be generated
using [gtfs-shape-mapfit](https://github.com/tru-hy/gtfs_shape_mapfit) and imported
with:

    PYTHONPATH=../../../ ./merge.bash coordinate_shape <path to noded shapes.txt>

Departures, meaning the physical drives, which are mapped to live
data require additional mangling as the live data is not necessarily
as such compatible with the GTFS schema. This mangling is done by
"schema adapters". Currently only (working) such adapter is for
City of Tampere's live SIRI data. To load those departures, run:

    PYTHONPATH=../../../ ./merge.bash transit_departure <gtfs directory> ../schema_adapters/tampere.py

#### Live data

Currently a City of Tampere's SIRI realtime data is the only live data
format supported. To import it, run

    cd utils/siri
    cat <siri dump file> |PYTHONPATH=../../../ ./siri_dump_parse.py load ../schema_adapters/tampere.py

*NOTE* The datasets are quite large and thus this will take quite a while.
Especially as the parsing isn't as efficient as it could.

### Process data

The dumped data needs to be mapped to the known routes for efficient
visualization. This is done with:

    cd utils
    PYTHONPATH=../.. ./filter_routes.py filter-stop-sequences
    PYTHONPATH=../.. ./filter_routes.py filter-routes

### Run the server

If all went well, you can issue `./rest_server.py` from the project
root and with default settings will find something by pointing your
browser to `http://localhost:8080`. Create `config_local.py` in the source
root to override the settings.py for something more suitable.

Note that there's no built in trickery to run the server in a privileged
port. One, albeit somewhat risky, way to accomplish this without running
the whole shebang as superuser is to grant the privileged port binding
capability to `.virtualenv/bin/python` by commanding
`sudo setcap 'cap_net_bind_service=+ep' .virtualenv/bin/python`.

