#!/bin/bash

# Configuration
BASE_RAFT_PORT=7000
BASE_SERVER_PORT=8000
DATA_DIR="./benchmark_data"
NUM_SERVERS=$(nproc)  # Get number of CPU cores
BENCH_DURATION="1m"

# Cleanup function
cleanup() {
    echo "Cleaning up..."
    # Kill all server processes
    if [ -f "server.pid" ]; then
        while read -r pid; do
            kill -15 "$pid" 2>/dev/null
        done < server.pid
        rm server.pid
    fi
    # Remove data directories
    rm -rf "$DATA_DIR"_*
}

# Set up trap for cleanup
trap cleanup EXIT INT TERM

# Ensure executables exist
if [ ! -f "./server" ] || [ ! -f "./benchmark" ]; then
    echo "Building executables..."
    go build -o server ./cmd/server
    go build -o benchmark ./cmd/bench/throughput
fi

# Create data directories and start servers
echo "Starting $NUM_SERVERS servers..."

# Start the first node (bootstrap node)
FIRST_DATA_DIR="${DATA_DIR}_1"
mkdir -p "$FIRST_DATA_DIR"
./server -id "node1" \
    -raft-addr "localhost:$BASE_RAFT_PORT" \
    -server-addr "localhost:$BASE_SERVER_PORT" \
    -data-dir "$FIRST_DATA_DIR" \
    -bootstrap > "$FIRST_DATA_DIR/server.log" 2>&1 &
echo $! >> server.pid

# Wait for the first node to start
sleep 5

# Start additional nodes
for ((i=2; i<=NUM_SERVERS; i++)); do
    RAFT_PORT=$((BASE_RAFT_PORT + i - 1))
    SERVER_PORT=$((BASE_SERVER_PORT + i - 1))
    NODE_DATA_DIR="${DATA_DIR}_${i}"

    mkdir -p "$NODE_DATA_DIR"

    ./server -id "node$i" \
        -raft-addr "localhost:$RAFT_PORT" \
        -server-addr "localhost:$SERVER_PORT" \
        -data-dir "$NODE_DATA_DIR" \
        -join "localhost:$BASE_SERVER_PORT" > "$NODE_DATA_DIR/server.log" 2>&1 &
    echo $! >> server.pid

    # Wait for node to start
    sleep 1
done

# Wait for cluster to stabilize
echo "Waiting for cluster to stabilize..."
sleep 5

# Run different benchmark scenarios
echo "Starting benchmarks..."

# Scenario 1: Write-heavy workload
echo "Running write-heavy benchmark..."
./benchmark -addr "localhost:$BASE_SERVER_PORT" \
    -n 100000 \
    -c 100 \
    -size 1024 \
    -write-ratio 0.8 \
    -duration "$BENCH_DURATION"

sleep 2

# Scenario 2: Read-heavy workload
echo "Running read-heavy benchmark..."
./benchmark -addr "localhost:$BASE_SERVER_PORT" \
    -n 100000 \
    -c 100 \
    -size 1024 \
    -write-ratio 0.2 \
    -duration "$BENCH_DURATION"

sleep 2

# Scenario 3: High concurrency
echo "Running high concurrency benchmark..."
./benchmark -addr "localhost:$BASE_SERVER_PORT" \
    -n 100000 \
    -c 1000 \
    -size 1024 \
    -write-ratio 0.5 \
    -duration "$BENCH_DURATION"

sleep 2

# Scenario 4: Large values
echo "Running large values benchmark..."
./benchmark -addr "localhost:$BASE_SERVER_PORT" \
    -n 10000 \
    -c 50 \
    -size 1048576 \
    -write-ratio 0.5 \
    -duration "$BENCH_DURATION"