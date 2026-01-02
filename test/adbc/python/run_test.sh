#!/bin/bash
# Helper script to run Python ADBC tests against Spice.ai using uv
#
# Usage:
#   ./run_test.sh [OPTIONS]
#
# Options:
#   --port PORT       Spice Flight SQL port (default: 50051)
#   --host HOST       Spice host (default: 127.0.0.1)
#   --install         Install Python dependencies first
#   --use-pip         Use pip instead of uv
#   --start-spiced    Start spiced in this directory before running tests
#   --help            Show this help message

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
HOST="127.0.0.1"
PORT="50051"
INSTALL=false
USE_PIP=false
START_SPICED=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --port)
            PORT="$2"
            shift 2
            ;;
        --host)
            HOST="$2"
            shift 2
            ;;
        --install)
            INSTALL=true
            shift
            ;;
        --use-pip)
            USE_PIP=true
            shift
            ;;
        --start-spiced)
            START_SPICED=true
            shift
            ;;
        --help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  --port PORT       Spice Flight SQL port (default: 50051)"
            echo "  --host HOST       Spice host (default: 127.0.0.1)"
            echo "  --install         Install Python dependencies first"
            echo "  --use-pip         Use pip instead of uv"
            echo "  --start-spiced    Start spiced in this directory before running tests"
            echo "  --help            Show this help message"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Check if uv or python3 is available
if [ "$USE_PIP" = false ] && command -v uv &> /dev/null; then
    PYTHON_CMD="uv run"
    echo "Using uv for Python execution"
elif command -v python3 &> /dev/null; then
    PYTHON_CMD="python3"
    echo "Using python3 for execution"
else
    echo "Error: Neither uv nor python3 is installed or in PATH"
    echo "Install uv: curl -LsSf https://astral.sh/uv/install.sh | sh"
    echo "Or install python3: https://www.python.org/downloads/"
    exit 1
fi

# Install dependencies if requested
if [ "$INSTALL" = true ]; then
    echo "Installing Python dependencies..."
    if [ "$USE_PIP" = false ] && command -v uv &> /dev/null; then
        uv pip install -r "$SCRIPT_DIR/requirements.txt"
    else
        python3 -m pip install -r "$SCRIPT_DIR/requirements.txt"
    fi
fi

# Check if dependencies are installed (only when not using uv run)
if [ "$PYTHON_CMD" = "python3" ]; then
    if ! python3 -c "import adbc_driver_flightsql" &> /dev/null; then
        echo "Error: adbc-driver-flightsql is not installed"
        echo "Run with --install to install dependencies, or run:"
        if command -v uv &> /dev/null; then
            echo "  uv pip install -r requirements.txt"
        else
            echo "  pip install -r requirements.txt"
        fi
        exit 1
    fi
fi

# Start spiced if requested
SPICED_PID=""
if [ "$START_SPICED" = true ]; then
    echo "Starting spiced in $SCRIPT_DIR..."
    
    # Check if spiced is in PATH or use the one in ~/.spice/bin
    if command -v spiced &> /dev/null; then
        SPICED_BIN="spiced"
    elif [ -f "$HOME/.spice/bin/spiced" ]; then
        SPICED_BIN="$HOME/.spice/bin/spiced"
    else
        echo "Error: spiced not found in PATH or ~/.spice/bin/spiced"
        echo "Please install spiced or add it to your PATH"
        exit 1
    fi
    
    # Kill any existing spiced on the port
    lsof -ti:$PORT | xargs kill -9 2>/dev/null || true
    sleep 1
    
    # Start spiced in the test directory (to use local spicepod.yaml)
    cd "$SCRIPT_DIR"
    $SPICED_BIN --flight 127.0.0.1:$PORT > /tmp/spiced_test.log 2>&1 &
    SPICED_PID=$!
    
    echo "Started spiced with PID $SPICED_PID"
    echo "Waiting for spiced to be ready..."
    
    # Wait for spiced to start (up to 10 seconds)
    for i in {1..20}; do
        if lsof -i:$PORT > /dev/null 2>&1; then
            echo "Spiced is ready and listening on port $PORT"
            break
        fi
        if ! ps -p $SPICED_PID > /dev/null 2>&1; then
            echo "Error: spiced process died. Log contents:"
            cat /tmp/spiced_test.log
            exit 1
        fi
        sleep 0.5
    done
    
    # Final check
    if ! lsof -i:$PORT > /dev/null 2>&1; then
        echo "Error: spiced is not listening on port $PORT after 10 seconds"
        echo "Log contents:"
        cat /tmp/spiced_test.log
        kill $SPICED_PID 2>/dev/null || true
        exit 1
    fi
    
    # Set up cleanup trap
    trap "echo 'Stopping spiced...'; kill $SPICED_PID 2>/dev/null || true" EXIT
fi

# Run the tests
echo "Running Python ADBC tests against Spice.ai..."
echo "Host: $HOST"
echo "Port: $PORT"
echo ""

$PYTHON_CMD "$SCRIPT_DIR/test_flightsql_adbc.py" --host "$HOST" --port "$PORT"
