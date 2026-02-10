#!/bin/bash

# Graceful shutdown script for FastAPI/Uvicorn server

# 1. Try graceful shutdown first
API_PID=$(lsof -t -i :8000)
if [ -n "$API_PID" ]; then
    echo "Attempting graceful shutdown of PID $API_PID..."
    kill -SIGTERM $API_PID
    
    # Wait up to 10 seconds for normal termination
    timeout=10
    while [ $timeout -gt 0 ]; do
        if ! ps -p $API_PID > /dev/null; then
            echo "Server stopped gracefully"
            exit 0
        fi
        sleep 1
        ((timeout--))
    done
    
    # 2. Force kill if still running
    echo "Force killing PID $API_PID..."
    kill -9 $API_PID
    sleep 1
fi

# 3. Final verification
if lsof -i :8000 > /dev/null; then
    echo "Failed to stop server"
    exit 1
else
    echo "Server successfully stopped"
    exit 0
fi
