#!/bin/bash
set -e

## This script runs all queries in a folder and checks if they pass or fail.
## Usage: ./run-queries.bash <query_folder>

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

if [ -f "./spice" ]; then
  echo "Using local spice binary"
  SPICE="./spice"
elif command -v spice &> /dev/null; then
  echo "Using system spice binary"
  SPICE="spice"
else
  echo "'spice' is required"
  exit 1
fi

query_folder=$1;
failed_queries=()
# ignore directories, only run queries from files
for i in `ls -d $query_folder/*.sql`; do
  echo "Running query in $i.."
  sed '/^--/d' < $i > $i.tmp # remove comments because we compact the query into one line
  tr '\n' ' ' <  $i.tmp | $SPICE sql > runqueries.tmp.txt

  rm -f $i.tmp
  result=`cat runqueries.tmp.txt`
  echo "$result"
  # if result contains error string, then it failed
  if [[ $result == *"Query Error"* ]] || [[ $result == *"Error"* ]]; then
    failed_queries+=($i)
  fi
done

if [ ${#failed_queries[@]} -eq 0 ]; then
  echo "All queries passed!"
  exit 0
else
  echo "Failed queries:"
  for i in ${failed_queries[@]}; do
    echo $i
  done
  exit 1
fi
