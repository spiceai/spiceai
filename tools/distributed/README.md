# Distributed Spice Cluster Manager

A production-quality Rust CLI tool for managing distributed Spice clusters with TLS/mTLS support, health checks, and log management.

## Overview

The `distributed` tool simplifies the management of a distributed Spice cluster consisting of:
- 1 Scheduler node
- N Executor nodes (default: 3)

It provides intuitive commands for starting, stopping, monitoring, and viewing logs of your cluster with automatic TLS certificate management and health checks.

## Platform Support

**Supported:** macOS, Linux (and other Unix-like systems)

**Not Supported:** Windows

This tool uses Unix-specific process management features (POSIX signals) and is not compatible with Windows. For Windows users, please use the Docker-based Spice cluster setup or the WSL (Windows Subsystem for Linux) environment.

## Installation

### Using Make (Recommended)

From the repository root:

```bash
# Build and install debug version
make install-distributed-dev

# Build and install release version
make install-distributed
```

The binary will be installed to `~/.spice/bin/distributed`.

### Using Cargo Directly

```bash
# Build debug version
cargo build -p distributed

# Build release version
cargo build --release -p distributed
```

The binary will be available at `target/debug/distributed` or `target/release/distributed`.

## Quick Start

```bash
# Start a cluster with default settings (3 executors)
distributed start

# In another terminal, check cluster status
distributed status

# View logs for a specific component
distributed logs scheduler
distributed logs executor1

# Stop the cluster
distributed stop
```

## Commands

### `distributed start`

Start a distributed Spice cluster in background or detached mode.

**Options:**

- `-e, --executors <N>` - Number of executors (default: 3)
- `-s, --scheduler-http <PORT>` - Scheduler HTTP port (default: 8090)
- `--scheduler-flight <PORT>` - Scheduler Flight port (default: 50051)
- `--scheduler-node <PORT>` - Scheduler node port (default: 50052)
- `--executor-http <PORT>` - Base executor HTTP port (default: 9090)
- `--executor-node <PORT>` - Base executor node port (default: 50062)
- `--log-dir <PATH>` - Log directory (default: ~/.spice/distributed/logs)
- `--work-dir <PATH>` - Working directory (default: ~/.spice/distributed)
- `--project-dir <PATH>` - Project directory (default: .)
- `--spiced-path <PATH>` - Path to spiced binary (default: ~/.spice/bin/spiced)
- `--no-tls-init` - Skip automatic TLS initialization
- `--no-health-check` - Skip health checks after startup
- `-d, --detach` - Start and exit (don't wait for Ctrl+C)

**Execution Modes:**

- **Background mode (default)**: Starts the cluster, shows status, and waits for Ctrl+C to stop
- **Detach mode (`--detach`)**: Starts the cluster, shows status, and exits immediately

**Examples:**

```bash
# Start with default settings and wait for Ctrl+C
distributed start

# Start 5 executors
distributed start --executors 5

# Start on custom ports
distributed start --scheduler-http 8091 --executor-http 9091

# Start and detach (run in background)
distributed start --detach

# Start without health checks (faster startup)
distributed start --no-health-check
```

### `distributed stop`

Stop a running cluster gracefully.

**Options:**

- `--work-dir <PATH>` - Working directory (default: ~/.spice/distributed)
- `--force` - Force kill processes if graceful shutdown fails
- `--timeout <SECS>` - Timeout for graceful shutdown in seconds (default: 10)

**Examples:**

```bash
# Gracefully stop the cluster
distributed stop

# Force stop if graceful shutdown doesn't work
distributed stop --force

# Use a longer timeout for graceful shutdown
distributed stop --timeout 30
```

### `distributed status`

Show cluster health status.

**Options:**

- `--work-dir <PATH>` - Working directory (default: ~/.spice/distributed)
- `--json` - Output status as JSON

**Examples:**

```bash
# Show status in human-readable format
distributed status

# Output status as JSON (useful for scripting)
distributed status --json
```

**Output:**

```
Cluster Status:

Scheduler:
  scheduler (port 8090): ✓ healthy

Executors:
  executor1 (port 9090): ✓ healthy
  executor2 (port 9091): ✓ healthy
  executor3 (port 9092): ✓ healthy
```

### `distributed logs`

View logs for a specific component.

**Arguments:**

- `<COMPONENT>` - Component to show logs for (scheduler, executor1, executor2, etc.)

**Options:**

- `--log-dir <PATH>` - Log directory (default: ~/.spice/distributed/logs)
- `--work-dir <PATH>` - Working directory (default: ~/.spice/distributed)
- `-f, --follow` - Follow log output (tail -f style)
- `-n <N>` - Number of lines to show from end (default: 50)

**Examples:**

```bash
# View last 50 lines of scheduler logs
distributed logs scheduler

# View last 100 lines of executor1 logs
distributed logs executor1 -n 100

# View last 20 lines and follow
distributed logs scheduler -n 20 -f

# Follow scheduler logs in real-time
distributed logs scheduler -f

# Follow executor2 logs
distributed logs executor2 --follow
```

## Architecture

### File Structure

```
~/.spice/
├── distributed/              # Working directory
│   ├── cluster.state        # Cluster state file
│   ├── scheduler/           # Scheduler working directory
│   ├── executor1/           # Executor 1 working directory
│   ├── executor2/           # Executor 2 working directory
│   ├── executor3/           # Executor 3 working directory
│   └── logs/                # Log directory
│       ├── scheduler.log    # Scheduler logs
│       ├── executor1.log    # Executor 1 logs
│       ├── executor2.log    # Executor 2 logs
│       └── executor3.log    # Executor 3 logs
└── pki/                     # TLS certificates
    ├── ca.crt               # CA certificate
    ├── ca.key               # CA private key
    ├── scheduler1.crt       # Scheduler certificate
    ├── scheduler1.key       # Scheduler private key
    ├── executor1.crt        # Executor 1 certificate
    ├── executor1.key        # Executor 1 private key
    └── ...                  # Additional executor certificates
```

### State Management

The tool maintains a `~/.spice/distributed/cluster.state` JSON file to track running processes:

```json
{
  "version": "1.0",
  "started_at": "2026-02-13T14:00:00Z",
  "project_dir": "/path/to/project",
  "scheduler": {
    "name": "scheduler",
    "pid": 12345,
    "http_port": 8090,
    "flight_port": 50051,
    "node_port": 50052,
    "work_dir": "/Users/username/.spice/distributed/scheduler",
    "log_file": "/Users/username/.spice/distributed/logs/scheduler.log"
  },
  "executors": [
    {
      "name": "executor1",
      "pid": 12346,
      "http_port": 9090,
      "node_port": 50062,
      "work_dir": "/Users/username/.spice/distributed/executor1",
      "log_file": "/Users/username/.spice/distributed/logs/executor1.log"
    }
  ]
}
```

### TLS Certificates

The tool automatically initializes TLS certificates using the `spice cluster tls` commands:

1. Initializes CA if not present (`spice cluster tls init`)
2. Generates certificates for scheduler and all executors (`spice cluster tls add <node-name>` for each node)

You can skip TLS initialization with `--no-tls-init` if certificates are already set up.

### Health Checks

After starting each node, the tool polls the `/health` endpoint to ensure the node is ready:

- **Max attempts**: 30
- **Interval**: 1 second
- **Timeout**: 2 seconds per request

Health checks can be skipped with `--no-health-check` for faster startup during development.

## Error Handling

The tool provides clear error messages for common issues:

- **Missing spiced binary**: Shows path and installation instructions
- **Cluster already running**: Suggests using `distributed stop` first
- **Health check failures**: Shows last 10 lines of log file
- **Port conflicts**: Surfaces errors from the runtime; adjust ports manually as needed
- **TLS initialization errors**: Shows error and suggests manual initialization with `spice cluster tls` commands
- **Missing TLS files**: When using `--no-tls-init`, ensure certificates exist before starting

## Process Management

- **Graceful shutdown**: Sends SIGTERM and waits for timeout
- **Force shutdown**: Sends SIGKILL if graceful shutdown fails
- **Process lifecycle**: Tracks PIDs and automatically cleans up on exit
- **Signal handling**: Ctrl+C gracefully stops all processes in background mode

## Comparison with Bash Script

The `distributed` tool replaces the `scripts/distributed.sh` bash script with:

| Feature | Bash Script | Rust CLI Tool |
|---------|-------------|---------------|
| Port configuration | Hard-coded | Fully configurable |
| Error handling | Basic | Comprehensive with clear messages |
| Process management | Limited | Full lifecycle with graceful shutdown |
| Health checks | Basic | Robust with retries and timeouts |
| State tracking | None | JSON state file in ~/.spice |
| Status command | No | Yes |
| Logs command | No | Yes with follow mode |
| Detach mode | No | Yes |
| Location | Local directories | Centralized in ~/.spice |

## Development

### Building

Using Make:

```bash
# Debug build
make build-distributed-dev

# Release build
make build-distributed

# Install debug build
make install-distributed-dev

# Install release build
make install-distributed
```

Using Cargo directly:

```bash
# Debug build
cargo build -p distributed

# Release build
cargo build --release -p distributed

# Run tests
cargo test -p distributed
```

### Code Structure

```
tools/distributed/
├── src/
│   ├── main.rs              # CLI entry point
│   ├── output.rs            # Colored output helpers
│   ├── commands/
│   │   ├── start.rs         # Start command
│   │   ├── stop.rs          # Stop command
│   │   ├── status.rs        # Status command
│   │   └── logs.rs          # Logs command
│   └── cluster/
│       ├── config.rs        # Configuration types
│       ├── state.rs         # State file management
│       ├── process.rs       # Process lifecycle
│       ├── health.rs        # Health checking
│       └── tls.rs           # TLS management
└── Cargo.toml
```

## Troubleshooting

### Cluster won't start

```bash
# Check if cluster is already running
distributed status

# If stuck, force stop
distributed stop --force

# Check logs for errors
distributed logs scheduler
distributed logs executor1
```

### Port conflicts

```bash
# Use alternative ports
distributed start --scheduler-http 8091 --executor-http 9091
```

### TLS issues

```bash
# Manually initialize TLS
spice cluster tls init

# Generate certificates for scheduler and all executors
spice cluster tls add scheduler1
spice cluster tls add executor1
spice cluster tls add executor2
spice cluster tls add executor3
# ... repeat 'add' command for each executor node
```

### Process won't stop

```bash
# Force kill all processes
distributed stop --force
```

## License

Apache License 2.0 - See LICENSE file in the repository root.
