#!/bin/bash

### Runs the suite of queries at the given scale factor while running spice and the queries at the same time
### Usage: ./setup-and-query.sh ../../crates/runtime/benches/queries/tpcds tpcds 1 <engine> <pg_port> <pg_host> <pg_user> <pg_pass> <pg_sslmode> <pg_db>

# Cleanup generated files
rm *.db 2> /dev/null

# Cleanup spicepod.yaml
rm spicepod.yaml 2> /dev/null

subset_args=("${@:2}")

# Setup the TPC-DS or TPC-H dataset, passing all args except the query folder
./setup-tpc-spicepod.bash "${subset_args[@]}" &
#./setup-tpc-spicepod.bash "${subset_args[@]}"
PID=$!
ECODE=$?

if [ $ECODE -ne 0 ]; then
  echo "Failed to start spice"
  exit 1  # Exit with an error code
fi

echo "Setup script Exit code is $ECODE"

# Start a timer
START_TIME=$(date +%s)

# Timeout after 10 minutes
MAX_WAIT_TIME=600

# Set the interval between checks (e.g., 5 seconds)
CHECK_INTERVAL=5

echo "Waiting for spice to load datasets..."

while true; do
    RESPONSE=$(curl -s http://localhost:8090/v1/ready)
    RCODE=$?

    if [[ "$RESPONSE" == "Ready" ]]; then
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

# Run the queries
echo "Running $2 queries..."
./run-queries.bash $1
EXIT_CODE=$?

if kill -0 "$PID" 2>/dev/null; then
  kill -TERM "$PID"
  wait "$PID"  # Wait for it to exit gracefully
  echo "spice terminated gracefully."
else
  echo "spice is not running, check the log"
  exit 1
fi

# Exit with the exit code of the query script
#exit $EXIT_CODE
exit 0;