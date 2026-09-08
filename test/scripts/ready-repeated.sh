#!/bin/bash

### This script is used to test and output the readiness status of spiced during startup

# Start spiced in the background
spiced &
# Retain the PID of spiced
SPICED_PID=$!

# Function to make curl requests and print HTTP status
make_requests() {
  end=$((SECONDS+10))
  while [ $SECONDS -lt $end ]; do
    # Make a curl request and print the HTTP status
    curl -o /dev/null -s -w "%{http_code}\n" http://localhost:8090/v1/ready
  done
}

# Start making curl requests
make_requests

# Stop spiced
kill $SPICED_PID
