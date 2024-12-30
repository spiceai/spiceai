#!/bin/bash

# Function to compare two files
compare_files() {
    local file1=$1
    local file2=$2

    if ! cmp -s "$file1" "$file2"; then
        echo "Files $file1 and $file2 are different."
    fi
}

# Loop through numbers 1 to 99
for i in $(seq 1 99); do
    file_pg="/Users/qianqian/spiceai/crates/runtime/benches/snapshots/bench__file_tpcds_q$i.snap"
    s3_pg="/Users/qianqian/spiceai/crates/runtime/benches/snapshots/bench__databricks_delta_tpcds_q$i.snap"

    # Check if both files exist
    if [[ -f "$file_pg" && -f "$s3_pg" ]]; then
        compare_files "$file_pg" "$s3_pg"
    else
        echo "One or both files $file_pg and $s3_pg do not exist."
    fi
done