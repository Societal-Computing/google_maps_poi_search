#!/bin/bash

# Check if Redis is running
if ! redis-cli ping | grep -q "PONG"; then
    echo "Redis server is not running."
    exit 0
fi

# Stop Redis server
echo "Stopping Redis server..."
redis-cli shutdown

# Verify that Redis has been stopped
if redis-cli ping &> /dev/null; then
    echo "Failed to stop Redis server."
    exit 1
else
    echo "Redis server stopped successfully."
fi
