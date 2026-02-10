#!/usr/bin/env python3
"""
Helper script to write logs to parquet files with daily/hourly structure.
Usage:
    echo "log message" | python3 log_to_parquet.py
    python3 log_to_parquet.py "log message"
    python3 log_to_parquet.py --level INFO "log message"
"""

import sys
import os
from datetime import datetime, timezone
import pandas as pd
from pathlib import Path

def get_log_path(base_dir="/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/logs", run_start_time=None):
    """Get the log file path based on run start time or current date and hour (UTC-based).
    
    Args:
        base_dir: Base directory for logs
        run_start_time: Optional datetime object (UTC) representing when the run started.
                       If provided, all logs for this run will go to the file based on this time.
                       If None, uses current time.
    """
    # Use run_start_time if provided, otherwise use current time
    if run_start_time is not None:
        time_basis = run_start_time
    else:
        time_basis = datetime.now(timezone.utc)
    
    date_str = time_basis.strftime("%Y%m%d")
    hour_str = time_basis.strftime("%H00")  # Round to hour (HH00 format, UTC)
    
    # Create daily folder: logs/cron_busyness_shared_YYYYMMDD/
    daily_folder = os.path.join(base_dir, f"cron_busyness_shared_{date_str}")
    os.makedirs(daily_folder, exist_ok=True)
    
    # Create hourly parquet file: YYYYMMDD_HH00.parquet
    log_file = os.path.join(daily_folder, f"{date_str}_{hour_str}.parquet")
    
    return log_file

def append_log_to_parquet(message, level="INFO", log_file=None, run_start_time=None):
    """Append a log entry to the parquet file.
    
    Args:
        message: Log message to write
        level: Log level (INFO, ERROR, etc.)
        log_file: Optional explicit log file path. If None, will be determined from run_start_time or current time.
        run_start_time: Optional datetime object (UTC) representing when the run started.
                       If provided, all logs for this run will go to the file based on this time.
    """
    try:
        if log_file is None:
            log_file = get_log_path(run_start_time=run_start_time)
        
        # Create new log entry with UTC timestamp (actual timestamp of the log entry)
        now = datetime.now(timezone.utc)
        new_entry = pd.DataFrame([{
            "timestamp": now.isoformat().replace("+00:00", "Z"),
            "level": level,
            "message": str(message)
        }])
        
        # Append to existing file or create new one
        if os.path.exists(log_file):
            try:
                existing_df = pd.read_parquet(log_file)
                combined_df = pd.concat([existing_df, new_entry], ignore_index=True)
                combined_df.to_parquet(log_file, index=False)
            except Exception as e:
                # If reading fails, create new file
                new_entry.to_parquet(log_file, index=False)
        else:
            new_entry.to_parquet(log_file, index=False)
    except Exception as e:
        # Fallback: write to stderr if parquet writing fails
        print(f"ERROR: Failed to write log to parquet: {e}", file=sys.stderr)
        print(f"Message was: {message}", file=sys.stderr)
        raise

def main():
    """Main entry point."""
    # Parse command line arguments
    level = "INFO"
    message = None
    run_start_time = None
    
    if len(sys.argv) > 1:
        args = sys.argv[1:]
        if "--level" in args:
            idx = args.index("--level")
            if idx + 1 < len(args):
                level = args[idx + 1]
                args = args[:idx] + args[idx+2:]
        if "--run-start-time" in args:
            idx = args.index("--run-start-time")
            if idx + 1 < len(args):
                try:
                    # Parse ISO format timestamp (e.g., "2026-01-20T17:30:00Z" or "2026-01-20T17:30:00+00:00")
                    time_str = args[idx + 1]
                    run_start_time = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
                    if run_start_time.tzinfo is None:
                        run_start_time = run_start_time.replace(tzinfo=timezone.utc)
                    args = args[:idx] + args[idx+2:]
                except Exception as e:
                    print(f"WARNING: Failed to parse --run-start-time: {e}", file=sys.stderr)
        if args:
            message = " ".join(args)
    
    # If no message from args, read from stdin
    if message is None:
        message = sys.stdin.read().strip()
    
    if message:
        append_log_to_parquet(message, level, run_start_time=run_start_time)
    else:
        print("No log message provided", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main()

