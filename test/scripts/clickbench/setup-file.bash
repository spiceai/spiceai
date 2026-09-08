#!/bin/bash
set -e
set -o pipefail

# Create the folder
mkdir -p ./file-clickbench-spicepod

# Change to the created folder
cd ./file-clickbench-spicepod

# Download clickbench data
wget --continue 'https://datasets.clickhouse.com/hits_compatible/hits.parquet'

# test if spicepod.yaml exists
if [ -f "spicepod.yaml" ]; then
  echo "spicepod.yaml found. Aborting."
  exit 1
fi

echo "version: v1" >> spicepod.yaml
echo "kind: Spicepod" >> spicepod.yaml
echo "name: FileConnectorClickbench" >> spicepod.yaml

echo "datasets:" >> spicepod.yaml
echo "  - from: file:hits.parquet" >> spicepod.yaml
echo "    name: hits" >> spicepod.yaml