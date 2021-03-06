#!/bin/bash

export PGPASSWORD=transit

query=`python2 merge_command.py $1`
query=`echo $query |tr \\\n " "`

cmd=`echo $1 |tr _ -`-csv
./load.py $cmd $2 $3 |psql -h localhost -U transit transit -c "$query"
