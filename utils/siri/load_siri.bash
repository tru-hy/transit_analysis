#!/bin/bash

set -v
# Load data
./siri_dump_parse.py load "$1"
# Fit to shapes
../filter_routes.py filter-routes 
