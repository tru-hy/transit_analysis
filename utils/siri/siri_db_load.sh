TRACE_FIFO=/tmp/transit_traces.csv.fifo
#TRACE_FIFO=/dev/null

rm $TRACE_FIFO
mkfifo $TRACE_FIFO
psql -U transit transit -c "\\copy coordinate_trace from '$TRACE_FIFO'" &
cat /dev/stdin | python siri_dump_parse.py to-csv --trace-output "$TRACE_FIFO" $1 |pv -l |psql -U transit transit -c '\copy coordinate_measurement from stdin'
rm $TRACE_FIFO
