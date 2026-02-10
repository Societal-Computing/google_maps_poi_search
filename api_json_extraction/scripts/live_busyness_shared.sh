#!/bin/bash

LOG_HELPER="/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/scripts/log_to_parquet.py"
LOCK_FILE="/tmp/live_busyness_shared.lock"

# Capture run start time (UTC) at the beginning - this will be used for all log file naming
RUN_START_TIME_UTC=$(date -u +"%Y-%m-%dT%H:%M:%SZ")

log_to_parquet() {
    # Use system python3 for logging helper
    echo "$1" | python3 "$LOG_HELPER" --level "$2" --run-start-time "$RUN_START_TIME_UTC" 2>&1 || echo "Failed to log: $1" >&2
}

# Forward summary lines about row counts and progress updates
filter_and_forward() {
    local line="$1"
    if [[ "$line" == *"Working for "* && "$line" == *"open place ids out of"* ]]; then
        log_to_parquet "$line" "INFO"
        return
    fi
    if [[ "$line" == *"Progress: processed "* && "$line" == *"out of"* && "$line" == *"open place ids"* ]]; then
        log_to_parquet "$line" "INFO"
        return
    fi
    if [[ "$line" == *"Another instance"* && "$line" == *"already running"* ]]; then
        log_to_parquet "$line" "WARNING"
        return
    fi
    if [[ "$line" == *"job_"* && "$line" == *"_results:"* ]]; then
        log_to_parquet "$line" "INFO"
        return
    fi
    # drop everything else – we don't persist detailed logs to parquet
}

# Check if another instance is already running
if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        # Another instance is already running; do not write anything to parquet, just exit quietly.
        exit 0
    else
        # Stale lock file, remove it
        rm -f "$LOCK_FILE"
    fi
fi

# Create lock file
echo $$ > "$LOCK_FILE"
trap "rm -f '$LOCK_FILE'" EXIT

# --- Load Environment Variables ---
# Determine project root (two levels up from this script: testing_monitoring_system/api_json_extraction/scripts -> testing_monitoring_system)
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if [ -f "$PROJECT_ROOT/.env" ]; then
    export $(grep -v '^#' "$PROJECT_ROOT/.env" | xargs)
fi

# Fallback defaults if .env is missing (optional, or error out)
PROXY_USERNAME="${PROXY_USERNAME}"
PROXY_PASSWORD="${PROXY_PASSWORD}"

echo "Starting shared busyness extraction..."
echo "Date: $(date)"

# --- CONFIGURATION ---
INPUT_FILE="$PROJECT_ROOT/live_busyness_extraction/input/poi_results_cumulative.csv"
OUTPUT_FILE="$PROJECT_ROOT/live_busyness_extraction/output/placeholder.parquet"
LOG_HELPER="$PROJECT_ROOT/api_json_extraction/scripts/log_to_parquet.py"
LOCK_FILE="/tmp/live_busyness_shared.lock"

# Use system python3 (avoid uv in cron)
CMD_PY="python3"
export PYTHONPATH="$PROJECT_ROOT"

# Ensure log directory exists
mkdir -p "$PROJECT_ROOT/live_busyness_extraction/output"

# 1) Started time
log_to_parquet "Starting shared busyness extraction at $(date -u) (PID: $$)" "INFO"
cd "$PROJECT_ROOT" || {
    # If we cannot cd, just exit without writing additional parquet logs
    exit 1
}
export PYTHONPATH="$PROJECT_ROOT"
export PATH=$HOME/.local/bin:$PATH
export TZ=UTC

# CMD_PY is already set to system python3 above
# Run Python script and capture output; only filtered summary lines go to parquet
$CMD_PY \
  "$PROJECT_ROOT/live_busyness_extraction/main.py" \
  --proxy_username "$PROXY_USERNAME" \
  --proxy_password "$PROXY_PASSWORD" \
  --input_file "$INPUT_FILE" \
  --output_file "$OUTPUT_FILE" \
  --per_proxy_concurrency 8 2>&1 | while IFS= read -r line; do
    filter_and_forward "$line"
done
EXIT_CODE=${PIPESTATUS[0]}

# 3) Completion time (single line, regardless of success/failure)
log_to_parquet "Shared busyness extraction completed at $(date -u) (exit code: $EXIT_CODE)" "INFO"
