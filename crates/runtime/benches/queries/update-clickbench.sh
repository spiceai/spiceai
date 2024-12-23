#!/bin/bash

set -e

current_dir=$(pwd)
benches=(
    "datafusion:arrow"
)

cd /tmp
rm -rf ClickBench
git clone git@github.com:ClickHouse/ClickBench.git
cd ClickBench

for folder in "${benches[@]}"; do
    IFS=':' read -r -a bench <<< "$folder"
    echo "Updating queries for ${bench[0]}"
    cd ${bench[0]}
    mkdir queries
    awk -v dir=./queries '{print > dir "/q" NR ".sql"}' queries.sql
    rm -rf $current_dir/clickbench/${bench[1]}
    cp -r queries $current_dir/clickbench/${bench[1]}
    cd ..
done

cd /tmp
rm -rf ClickBench