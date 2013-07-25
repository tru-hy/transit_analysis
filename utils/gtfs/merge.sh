query=`python merge_command.py $1`
query=`echo $query |tr \\\n " "`

cmd=`echo $1 |tr _ -`-csv
python load.py $cmd $2 |psql -U transit transit -c "$query"
