#!/bin/bash

export PGPASSWORD=transit

query=`python merge_command.py $1`
query=`echo $query |tr \\\n " "`

cmd=`echo $1 |tr _ -`-csv
./load.py $cmd $2 |psql -h localhost -U transit transit -c "$query"
