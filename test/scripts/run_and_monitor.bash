#!/bin/bash

## This script runs the spice binary and monitors its resource usage
## Usage: ./run_and_monitor.sh <output_file>

spice run &

PID=$(pgrep -d',' -f spiced)

top -b -d 1 -p $PID >> $1	
