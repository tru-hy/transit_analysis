#!/bin/bash
set -v
# Install dependencies
sudo apt-get install `cat dependencies.debian`

# Build binary stuff
make

# Install Python dependencies
virtualenv --system-site-packages .virtualenv
source .virtualenv/bin/activate
pip install -r dependencies.pip

# Initialize database stuff
sudo su postgres -c ".virtualenv/bin/python -B schema.py create-user"
sudo su postgres -c ".virtualenv/bin/python -B schema.py create-database"
./schema.py initialize-schema
