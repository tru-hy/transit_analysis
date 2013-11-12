#!/bin/bash
set -v

./merge.bash transit_stop $1
./merge.bash transit_schedule_shape $1
./merge.bash transit_schedule_stop $1
./merge.bash transit_shape_stop $1
