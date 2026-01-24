#!/bin/bash

# Script to set up and start Spice scheduler and multiple executors with mTLS certificates
# Usage: ./setup_executors.sh <number_of_executors> [mode]
#   mode: 'background' (default) or 'foreground'

set -e

# Default number of executors if not provided
NUM_EXECUTORS=${1:-3}
MODE=${2:-background}

# Scheduler ports
SCHEDULER_HTTP_PORT=8090
SCHEDULER_FLIGHT_PORT=50051
SCHEDULER_NODE_PORT=50052

# Executor base ports
BASE_HTTP_PORT=9090
BASE_NODE_PORT=50062

# Scheduler address
SCHEDULER_ADDRESS="127.0.0.1:${SCHEDULER_NODE_PORT}"

# Colors for output
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

echo -e "${BLUE}Setting up Spice cluster with 1 scheduler and ${NUM_EXECUTORS} executors...${NC}\n"

# Check if spice CLI is available
if ! command -v spice &> /dev/null; then
    echo -e "${YELLOW}Warning: spice CLI not found in PATH${NC}"
    echo "Make sure Spice is installed or use ~/.spice/bin/spice directly"
fi

# Check if spiced binary exists
if [ ! -f ~/.spice/bin/spiced ]; then
    echo -e "${RED}Error: ~/.spice/bin/spiced not found${NC}"
    exit 1
fi

# Initialize CA if it doesn't exist
if [ ! -f ~/.spice/pki/ca.crt ]; then
    echo -e "${GREEN}Initializing Spice cluster TLS...${NC}"
    spice cluster tls init
    echo ""
fi

# Generate scheduler certificate if it doesn't exist
if [ ! -f ~/.spice/pki/scheduler1.crt ]; then
    echo -e "${GREEN}Generating scheduler certificate...${NC}"
    spice cluster tls add scheduler1
    echo ""
fi

# Generate TLS certificates for each executor
echo -e "${GREEN}Generating executor certificates...${NC}"
for i in $(seq 1 $NUM_EXECUTORS); do
    executor_name="executor${i}"

    # Check if certificate already exists
    if [ -f ~/.spice/pki/${executor_name}.crt ]; then
        echo "Certificate for ${executor_name} already exists, skipping..."
    else
        echo "Generating certificate for ${executor_name}..."
        spice cluster tls add ${executor_name}
    fi
done
echo ""

# Create logs directory
LOG_DIR="./cluster_logs"
mkdir -p ${LOG_DIR}

# Array to store PIDs
SCHEDULER_PID=""
declare -a EXECUTOR_PIDS

# Function to start scheduler
start_scheduler() {
    echo -e "${GREEN}Starting Spice Scheduler...${NC}"
    echo "  HTTP Port: ${SCHEDULER_HTTP_PORT}"
    echo "  Flight Port: ${SCHEDULER_FLIGHT_PORT}"
    echo "  Node Port: ${SCHEDULER_NODE_PORT}"

    if [ "$MODE" = "background" ]; then
        ~/.spice/bin/spiced --role scheduler \
          --node-bind-address 127.0.0.1:${SCHEDULER_NODE_PORT} \
          --node-advertise-address 127.0.0.1:${SCHEDULER_NODE_PORT} \
          --http 127.0.0.1:${SCHEDULER_HTTP_PORT} \
          --flight 127.0.0.1:${SCHEDULER_FLIGHT_PORT} \
          --node-mtls-ca-certificate-file ~/.spice/pki/ca.crt \
          --node-mtls-certificate-file ~/.spice/pki/scheduler1.crt \
          --node-mtls-key-file ~/.spice/pki/scheduler1.key \
          > ${LOG_DIR}/scheduler.log 2>&1 &

        SCHEDULER_PID=$!
        echo "  PID: ${SCHEDULER_PID}"
        echo "  Log: ${LOG_DIR}/scheduler.log"
    else
        echo -e "${YELLOW}  Run in a separate terminal:${NC}"
        echo "  ~/.spice/bin/spiced --role scheduler \\"
        echo "    --node-bind-address 127.0.0.1:${SCHEDULER_NODE_PORT} \\"
        echo "    --node-advertise-address 127.0.0.1:${SCHEDULER_NODE_PORT} \\"
        echo "    --http 127.0.0.1:${SCHEDULER_HTTP_PORT} \\"
        echo "    --flight 127.0.0.1:${SCHEDULER_FLIGHT_PORT} \\"
        echo "    --node-mtls-ca-certificate-file ~/.spice/pki/ca.crt \\"
        echo "    --node-mtls-certificate-file ~/.spice/pki/scheduler1.crt \\"
        echo "    --node-mtls-key-file ~/.spice/pki/scheduler1.key"
    fi
    echo ""
}

# Function to check if scheduler is ready
check_scheduler() {
    if curl -s http://127.0.0.1:${SCHEDULER_HTTP_PORT}/health > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Function to start an executor
start_executor() {
    local executor_num=$1
    local executor_name="executor${executor_num}"
    local http_port=$((BASE_HTTP_PORT + executor_num - 1))
    local node_port=$((BASE_NODE_PORT + executor_num - 1))

    echo -e "${GREEN}Starting ${executor_name}...${NC}"
    echo "  HTTP Port: ${http_port}"
    echo "  Node Port: ${node_port}"
    echo "  Connecting to scheduler: ${SCHEDULER_ADDRESS}"

    if [ "$MODE" = "background" ]; then
        ~/.spice/bin/spiced --role executor \
          --http 127.0.0.1:${http_port} \
          --scheduler-address ${SCHEDULER_ADDRESS} \
          --node-mtls-ca-certificate-file ~/.spice/pki/ca.crt \
          --node-mtls-certificate-file ~/.spice/pki/${executor_name}.crt \
          --node-mtls-key-file ~/.spice/pki/${executor_name}.key \
          --node-bind-address 127.0.0.1:${node_port} \
          --node-advertise-address 127.0.0.1:${node_port} \
          > ${LOG_DIR}/${executor_name}.log 2>&1 &

        local pid=$!
        EXECUTOR_PIDS+=($pid)
        echo "  PID: ${pid}"
        echo "  Log: ${LOG_DIR}/${executor_name}.log"
    else
        echo -e "${YELLOW}  Run in a separate terminal:${NC}"
        echo "  ~/.spice/bin/spiced --role executor \\"
        echo "    --http 127.0.0.1:${http_port} \\"
        echo "    --scheduler-address ${SCHEDULER_ADDRESS} \\"
        echo "    --node-mtls-ca-certificate-file ~/.spice/pki/ca.crt \\"
        echo "    --node-mtls-certificate-file ~/.spice/pki/${executor_name}.crt \\"
        echo "    --node-mtls-key-file ~/.spice/pki/${executor_name}.key \\"
        echo "    --node-bind-address 127.0.0.1:${node_port} \\"
        echo "    --node-advertise-address 127.0.0.1:${node_port}"
    fi
    echo ""
}

# Function to check executor health
check_executor() {
    local executor_num=$1
    local http_port=$((BASE_HTTP_PORT + executor_num - 1))

    if curl -s http://127.0.0.1:${http_port}/health > /dev/null 2>&1; then
        return 0
    else
        return 1
    fi
}

# Function to show recent log errors
show_log_errors() {
    echo ""
    echo -e "${YELLOW}Checking logs for errors...${NC}"

    if [ -f "${LOG_DIR}/scheduler.log" ]; then
        echo ""
        echo -e "${BLUE}Last 10 lines of scheduler.log:${NC}"
        tail -10 ${LOG_DIR}/scheduler.log
    fi

    for i in $(seq 1 $NUM_EXECUTORS); do
        local executor_name="executor${i}"
        if [ -f "${LOG_DIR}/${executor_name}.log" ]; then
            echo ""
            echo -e "${BLUE}Last 10 lines of ${executor_name}.log:${NC}"
            tail -10 ${LOG_DIR}/${executor_name}.log
        fi
    done
}

# Function to cleanup on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}Stopping all cluster components...${NC}"

    # Stop executors
    for pid in "${EXECUTOR_PIDS[@]}"; do
        if kill -0 $pid 2>/dev/null; then
            kill $pid
            echo "Stopped executor with PID: $pid"
        fi
    done

    # Stop scheduler
    if [ -n "$SCHEDULER_PID" ] && kill -0 $SCHEDULER_PID 2>/dev/null; then
        kill $SCHEDULER_PID
        echo "Stopped scheduler with PID: $SCHEDULER_PID"
    fi

    exit 0
}

# Main execution
if [ "$MODE" = "background" ]; then
    # Set up signal handlers for clean shutdown
    trap cleanup SIGINT SIGTERM

    # Start scheduler
    echo -e "${GREEN}Step 1: Starting Scheduler...${NC}"
    echo ""
    start_scheduler

    # Wait for scheduler to be ready
    echo "Waiting for scheduler to be ready..."
    for i in {1..30}; do
        if check_scheduler; then
            echo -e "${GREEN}✓ Scheduler is ready${NC}"
            break
        fi
        if [ $i -eq 30 ]; then
            echo -e "${RED}✗ Scheduler failed to start within 30 seconds${NC}"
            show_log_errors
            cleanup
            exit 1
        fi
        sleep 1
    done
    echo ""

    # Start executors
    echo -e "${GREEN}Step 2: Starting Executors...${NC}"
    echo ""
    for i in $(seq 1 $NUM_EXECUTORS); do
        start_executor $i
        sleep 2
    done

    echo -e "${BLUE}All cluster components started in background!${NC}"
    echo ""
    echo "Logs are available in ${LOG_DIR}/"
    echo ""

    # Wait a bit for executors to start
    echo "Waiting for executors to be ready..."
    sleep 5

    # Check status
    echo ""
    echo -e "${GREEN}Cluster Status:${NC}"
    echo ""
    echo "Scheduler:"
    echo -n "  scheduler (port ${SCHEDULER_HTTP_PORT}): "
    if check_scheduler; then
        echo -e "${GREEN}✓ Running${NC}"
    else
        echo -e "${RED}✗ Not responding${NC}"
    fi

    echo ""
    echo "Executors:"
    all_healthy=true
    for i in $(seq 1 $NUM_EXECUTORS); do
        http_port=$((BASE_HTTP_PORT + i - 1))
        echo -n "  executor${i} (port ${http_port}): "
        if check_executor $i; then
            echo -e "${GREEN}✓ Running${NC}"
        else
            echo -e "${RED}✗ Not responding${NC}"
            all_healthy=false
        fi
    done

    if [ "$all_healthy" = false ]; then
        show_log_errors
    fi

    echo ""
    echo -e "${YELLOW}Press Ctrl+C to stop all cluster components${NC}"
    echo ""
    echo "Useful commands:"
    echo "  # Check scheduler logs:"
    echo "  tail -f ${LOG_DIR}/scheduler.log"
    echo ""
    echo "  # Check executor logs:"
    echo "  tail -f ${LOG_DIR}/executor1.log"
    echo ""
    echo "  # Query the cluster:"
    echo "  curl -X POST http://127.0.0.1:${SCHEDULER_HTTP_PORT}/v1/sql \\"
    echo "    -H 'Content-Type: application/text' \\"
    echo "    -H 'Accept: text/plain' \\"
    echo "    -d 'show tables'"
    echo ""
    echo "  # Stop manually later:"
    echo "  pkill -f 'spiced --role'"

    # Keep script running
    wait
else
    # Foreground mode - just print commands
    echo -e "${GREEN}Step 1: Starting Scheduler...${NC}"
    echo ""
    start_scheduler

    echo -e "${GREEN}Step 2: Starting Executors (after scheduler is ready)...${NC}"
    echo ""
    for i in $(seq 1 $NUM_EXECUTORS); do
        start_executor $i
    done

    echo -e "${BLUE}Commands generated!${NC}"
    echo "Copy and run each command in a separate terminal, starting with the scheduler."
fi
