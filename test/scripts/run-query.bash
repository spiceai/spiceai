#!/bin/bash
set -e

clean_up () {
  ARG=$?
  rm -f runqueries.tmp.txt
  exit $ARG
}

if [ -z "$1" ]; then
  echo "No query folder provided. Aborting."
  echo "Usage: $0 <query_folder> <query_name>"
  exit 1
fi

if [ -z "$2" ]; then
  echo "No query name specified. Aborting."
  echo "Usage: $0 <query_folder> <query_name>"
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
i=$query_folder/$2.sql
echo "Running query in $i.."
sed '/^--/d' < $i > $i.tmp # remove comments because we compact the query into one line
tr '\n' ' ' <  $i.tmp | spice sql > runqueries.tmp.txt

rm $i.tmp
result=`cat runqueries.tmp.txt`
echo "$result"