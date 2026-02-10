#!/bin/bash

# Check if Redis is installed
if ! command -v redis-server &> /dev/null; then
    echo "Redis is not installed. Please install Redis first."
    exit 1
fi

# Start Redis server in the background
echo "Starting Redis server..."
redis-server --daemonize yes

# Check if Redis started successfully
if redis-cli ping | grep -q "PONG"; then
    echo "Redis server is running."
else
    echo "Failed to start Redis server."
    exit 1
fi
