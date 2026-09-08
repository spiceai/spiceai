#!/bin/bash

## This script runs the spice binary and monitors its resource usage
## Usage: ./run_and_monitor.sh <output_file>

## To use this script, first run:
## ./setup-tpc-spicepod.bash tpch 10
## Then start `spice` and monitor its resource usage with:
## ./run_spice_and_monitor.bash tpch_run.log
## Then run the queries in a separate terminal with:
## ./run_queries_and_monitor.bash <query_folder> tpch_run.log
## Then process the results with:
## ./process_results.bash tpch_run.log

# Run the spice binary
spice run &

# Get the PID of the spiced process
PID=$(pgrep -d',' -f spiced)

# Run `top` in batch mode every 1 second for the `spiced` process, output to the specified file
top -b -d 1 -p $PID >> $1	
