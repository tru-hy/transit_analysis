# Transit Analysis

A work in progress software system for analyzing transit data.
In very early development.


## Getting started

The system currently consists mostly of the database
schema and utilities to dump some data in to the database.

Requires Python 2.x (developed on 2.7), modern version of Postgresql
(developed on 9.2) and a bunch of more or less obscure Python
packages.

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


### Process data

TODO
