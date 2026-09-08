#!/bin/bash
set -e

## This script runs a set of queries against a running spice instance and checks for errors
## It will wait for Spice to be up and running, timing out after 10 minutes.
## At the start of every query, it will inject the query name into the current log file
## being used by run_and_monitor.sh
## To use this script, first run:
## ./setup-tpc-spicepod.bash tpch 10
## Then start `spice` and monitor its resource usage with:
## ./run_spice_and_monitor.bash tpch_run.log
## Then run the queries in a separate terminal with:
## ./run_queries_and_monitor.bash <query_folder> tpch_run.log
## Then process the results with:
## ./process_results.bash tpch_run.log

clean_up () {
  ARG=$?
  rm -f runqueries.tmp.txt
  exit $ARG
}

if [ -z "$1" ]; then
  echo "No query folder provided. Aborting."
  echo "Usage: $0 <query_folder>"
  exit 1
fi

# check if folder contains runqueries.tmp.txt
if [ -f runqueries.tmp.txt ]; then
  echo "runqueries.tmp.txt already exists. Aborting."
  exit 1
fi

trap clean_up EXIT

# Start a timer
START_TIME=$(date +%s)

# Timeout after 10 minutes
MAX_WAIT_TIME=600

# Set the interval between checks (e.g., 5 seconds)
CHECK_INTERVAL=5

echo "Waiting for spice to load datasets..."

# Wait for the datasets to load
while true; do
    RESPONSE=$(curl -s http://localhost:8090/v1/ready)
    RCODE=$?

    if [[ "$RESPONSE" == "ready" ]]; then
        echo "Datasets loaded!"
        break
    fi

    if [[ $RCODE -eq 7 ]]; then
        echo "spice is not responding to HTTP queries, check the log"
    fi

    CURRENT_TIME=$(date +%s)
    ELAPSED_TIME=$((CURRENT_TIME - START_TIME))

    if (( ELAPSED_TIME > MAX_WAIT_TIME )); then
        echo "Timed out waiting for spice datasets to load. Check /tmp/spice_tpc_run.log for more information."
        exit 1
    fi

    # Wait before the next check
    sleep $CHECK_INTERVAL
done

query_folder=$1;
failed_queries=()
for i in `ls -d $query_folder/**`; do
  echo "Running query in $i.."
  echo "//$i" >> $2
  sed '/^--/d' < $i > $i.tmp # remove comments because we compact the query into one line
  tr '\n' ' ' <  $i.tmp | spice sql > runqueries.tmp.txt

  rm $i.tmp
  result=`cat runqueries.tmp.txt`
  echo "$result"
  # if result contains error string, then it failed
  if [[ $result == *"Query Error"* ]] || [[ $result == *"Error"* ]]; then
    failed_queries+=($i)
  fi
done

if [ ${#failed_queries[@]} -eq 0 ]; then
  echo "All queries passed!"
else
  echo "Failed queries:"
  for i in ${failed_queries[@]}; do
    echo $i
  done
fi
