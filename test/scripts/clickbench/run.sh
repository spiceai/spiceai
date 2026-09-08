#!/bin/bash

TRIES=3
QUERY_NUM=1
failed_queries=()

# Read queries from queries.sql using input redirection
while read -r query; do
    # Clean system cache if running on linux
    # sync
    # echo 3 | sudo tee /proc/sys/vm/drop_caches >/dev/null

    echo "$query" > query.sql

    echo -n "["
    for i in $(seq 1 $TRIES); do
        tr '\n' ' ' < query.sql | spice sql > runqueries.tmp.txt
        result=$(cat runqueries.tmp.txt)

        RES=$(echo "$result" | awk '/Time:/ {print $2}')
        [[ $RES != "" ]] && \
            echo -n "$RES" || \
            echo -n "null"
        [[ "$i" != $TRIES ]] && echo -n ", "

        # If result contains error string, then it failed
        if [[ "$i" == $TRIES ]] && ([[ $result == *"Query Error"* ]] || [[ $result == *"ERROR"* ]]); then
            failed_queries+=($QUERY_NUM)
        fi

        echo "${QUERY_NUM},${i},${RES}" >> result.csv
    done
    echo "],"

    QUERY_NUM=$((QUERY_NUM + 1))
done < queries.sql

# Print the failed queries
echo "Failed Queries: ${failed_queries[@]}"