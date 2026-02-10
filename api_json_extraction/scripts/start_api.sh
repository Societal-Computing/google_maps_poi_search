
#!/bin/bash

# Navigate to the api_json_extraction directory (previous working context)
cd "$(dirname "$0")/.."

# Ensure this directory is the import root so 'app' and 'db' resolve
export PYTHONPATH=$(pwd)

# Defaults
PORT=8000
FORCE_KILL=0

# Parse args (POSIX sh compatible)
while [ $# -gt 0 ]; do
  case "$1" in
    --port)
      PORT="$2"; shift 2 ;;
    --force-kill)
      FORCE_KILL=1; shift 1 ;;
    *)
      shift 1 ;;
  esac
done

# If the chosen port is in use
if lsof -i :$PORT > /dev/null; then
    if [ "$FORCE_KILL" -eq 1 ]; then
        echo "Port $PORT is in use. Attempting to stop the process..."
        API_PID=$(lsof -t -i :$PORT)
        if [ -n "$API_PID" ]; then
            echo "Stopping process with PID $API_PID..."
            kill -9 $API_PID
            sleep 2
            if lsof -i :$PORT > /dev/null; then
                echo "Failed to stop the process using port $PORT."
                exit 1
            fi
        fi
    else
        echo "Port $PORT is already in use. Start with a different --port or pass --force-kill to replace it."
        exit 1
    fi
fi

# Start the FastAPI server (prefer uv, fallback to python3 -m uvicorn)
echo "Starting FastAPI server on port $PORT..."

# Ensure broken Python env vars don't leak into the server process
unset PYTHONHOME

if command -v uv >/dev/null 2>&1; then
    uv run -q uvicorn app.main:app \
        --app-dir . \
        --reload --reload-dir . \
        --host 0.0.0.0 --port "$PORT" &
else
    python3 -m uvicorn app.main:app \
        --app-dir . \
        --reload --reload-dir . \
        --host 0.0.0.0 --port "$PORT" &
fi

# Save the PID of the FastAPI server process
API_PID=$!

# Wait a moment to ensure the server starts
sleep 2

# Check if the server started successfully
if ps -p $API_PID > /dev/null; then
    echo "FastAPI server is running with PID $API_PID."
else
    echo "Failed to start FastAPI server."
    exit 1
fi