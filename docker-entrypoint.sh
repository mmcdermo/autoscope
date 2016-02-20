#!/bin/bash
set -e

args=()
# If empty, assume we want to run server
if [ -z "$1" ]; then
    echo "--------------------------------------------------------------"
    echo "Running autoscope"
    args=("autoscope")
else
    echo "--------------------------------------------------------------"
    echo "Running command: $@"
    args="$@"
fi

exec $args
