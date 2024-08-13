#!/bin/bash
set -e

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

query_folder=$1;
failed_queries=()
for i in `ls -d $query_folder/**`; do
  echo "Running query in $i.."
  tr '\n' ' ' <  $i | spice sql > runqueries.tmp.txt

  result=`cat runqueries.tmp.txt`
  echo "$result"
  # if result contains error string, then it failed
  if [[ $result == *"Query Error"* ]] || [[ $result == *"ERROR"* ]]; then
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
