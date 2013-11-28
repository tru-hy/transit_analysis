#!/bin/bash

SIRI_URL=$1
SIRI_NAME=$2
ADAPTER=$3
OUTPUT_FORMAT=$4

./siri_vehicle_logger.py "$SIRI_URL" "$SIRI_NAME" |
../logsplitter.py "$OUTPUT_FORMAT" |
while read file
do
	echo $file >&2
	bzcat $file |./load_siri.bash "$ADAPTER"
done
