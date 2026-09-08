#!/bin/bash

## Processes the results from run_and_monitor.bash into a CSV file
## Usage: ./process_results.bash <input_file>

## To use this script, first run:
## ./setup-tpc-spicepod.bash tpch 10
## Then start `spice` and monitor its resource usage with:
## ./run_spice_and_monitor.bash tpch_run.log
## Then run the queries in a separate terminal with:
## ./run_queries_and_monitor.bash <query_folder> tpch_run.log
## Then process the results with:
## ./process_results.bash tpch_run.log

set -e

if [ -z "$1" ]; then
  echo "No input file provided. Aborting."
  echo "Usage: $0 <input_file>"
  exit 1
fi

nl=$'\n'
HEADER=$(cat $1 | grep PID | head -n 1 | sed 's/  */,/g' | sed 's/^,//')
LINES=`grep "spiced" $1 | awk '{print $0,"\n\n"}' | sed 's/  */,/g' | sed 's/^,//' | sed 's/,$//'`
LINES2=($LINES)

echo $HEADER > $1.csv

for i in "${LINES2[@]}"
do
  :
  echo $i >> $1.csv
done

