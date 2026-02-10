import os
import json
from datetime import datetime, timezone
import logging
import argparse
import glob
from typing import List, Dict, Any, Set, Optional
from datetime import datetime as dt
import time
import sys
import csv
import requests
from live_busyness_extraction.extractor import LiveBusynessExtractor
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue
import random
from zoneinfo import ZoneInfo
import re
import threading
import functools
from pathlib import Path

# Resolve project root relative to this file
_THIS_FILE = Path(__file__).resolve()
_LBE_ROOT = _THIS_FILE.parent          # live_busyness_extraction
_PROJECT_ROOT = _LBE_ROOT.parent       # repository root


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# Force log timestamps to UTC for a common baseline across jobs
try:
    import time as _time
    logging.Formatter.converter = _time.gmtime
except Exception:
    pass
logger = logging.getLogger(__name__)

# Single-instance lock file (applies to any entrypoint that runs this main.py)
LOCK_FILE = "/tmp/live_busyness_extraction_main.lock"

# Caches to speed up opening-hours filtering across very large CSVs.
# - Many places share identical opening-hours strings (chains), so we memoize parsed schedules.
# - Timezone lookup via timezonefinder is expensive; memoize by rounded lat/lng.
_TZ_CACHE: Dict[tuple, Optional[str]] = {}


@functools.lru_cache(maxsize=200000)
def _parse_opening_hours_cached(hours_str: str) -> Dict[str, List[tuple]]:
    return _parse_opening_hours(hours_str)


def _acquire_single_instance_lock(lock_file: str) -> bool:
    """
    Ensure only one instance of this script is running at a time.
    Returns True if this process acquired the lock, False if another active
    instance already holds it.
    """
    try:
        if os.path.exists(lock_file):
            try:
                with open(lock_file, "r", encoding="utf-8") as f:
                    pid_str = f.read().strip()
                existing_pid = int(pid_str) if pid_str else None
            except Exception:
                existing_pid = None

            if existing_pid and existing_pid != os.getpid():
                try:
                    # Check if process is still alive
                    os.kill(existing_pid, 0)
                    logger.info(
                        f"Another live_busyness_extraction instance is already running "
                        f"(PID: {existing_pid}). Exiting this run."
                    )
                    return False
                except OSError:
                    # Stale PID; we'll overwrite the lock below
                    pass

        with open(lock_file, "w", encoding="utf-8") as f:
            f.write(str(os.getpid()))
        return True
    except Exception as e:
        logger.error(f"Failed to acquire single-instance lock {lock_file}: {e}")
        # If we can't be sure, be conservative and refuse to run
        return False


def _release_single_instance_lock(lock_file: str) -> None:
    """Release the single-instance lock if we own it."""
    try:
        if not os.path.exists(lock_file):
            return
        with open(lock_file, "r", encoding="utf-8") as f:
            pid_str = f.read().strip()
        existing_pid = int(pid_str) if pid_str else None
        if existing_pid == os.getpid():
            os.remove(lock_file)
    except Exception:
        # Don't crash on shutdown if we can't remove the lock.
        pass


# Optional, cached timezone finder instance for performance/thread safety
try:
    from timezonefinder import TimezoneFinder as _TFClass
    _TZ_FINDER = _TFClass(in_memory=True)
except Exception:
    _TZ_FINDER = None


def _infer_timezone_name(location: Dict[str, Any] = None, tz_hint: Optional[str] = None, city: Optional[str] = None) -> Optional[str]:
    """Infer an IANA timezone name strictly from explicit hints or coordinates.
    Returns None if it cannot be determined. No geographic fallbacks.
    """
    # explicit hint wins
    if tz_hint and isinstance(tz_hint, str) and tz_hint.strip():
        return tz_hint.strip()

    # coordinates via timezonefinder
    if location and isinstance(location, dict):
        lat_val = location.get('lat') or location.get('latitude')
        lng_val = location.get('lng') or location.get('longitude')
        try:
            if lat_val is not None and lng_val is not None:
                tf = _TZ_FINDER
                if tf is None:
                    from timezonefinder import TimezoneFinder
                    tf = TimezoneFinder(in_memory=True)
                tz = tf.timezone_at(lng=float(lng_val), lat=float(lat_val))
                if tz:
                    return tz
        except Exception:
            pass

    # unknown
    return None


def _format_local_time_from_utc(utc_dt: datetime, tz_name: Optional[str]) -> Optional[str]:
    """Format local time ISO string; returns None if tz_name is not provided or invalid."""
    if not tz_name:
        return None
    try:
        local_dt = utc_dt.astimezone(ZoneInfo(tz_name))
        return local_dt.strftime('%Y-%m-%dT%H:%M:%S')
    except Exception:
        return None


def get_latest_proxy_file() -> str:
    """Get the most recent proxy file from the working_proxies directory (resolved relative to this file)."""
    base_dir = _LBE_ROOT
    proxies_dir = os.path.join(base_dir, 'input', 'working_proxies')
    proxy_files = glob.glob(os.path.join(proxies_dir, 'working_proxies_*.txt'))
    if not proxy_files:
        raise FileNotFoundError(f"No proxy files found in working_proxies directory: {proxies_dir}")
    return max(proxy_files, key=os.path.getctime)

def load_place_ids(file_path: str) -> List[str]:
    """Load place IDs from a file."""
    try:
        # Handle CSV files
        if file_path.endswith('.csv'):
            place_ids = []
            with open(file_path, 'r') as f:
                reader = csv.DictReader(f)
                if 'place_id' not in reader.fieldnames:
                    raise ValueError("CSV file must contain 'place_id' column")
                for row in reader:
                    if row['place_id'] and row['place_id'].strip():
                        place_ids.append(row['place_id'].strip())
            return place_ids
        # Handle text files
        else:
            with open(file_path, 'r') as f:
                return [line.strip() for line in f if line.strip()]
    except Exception as e:
        logger.error(f"Error loading place IDs: {e}")
        raise

def csv_has_columns(file_path: str, required: List[str]) -> bool:
    try:
        if not file_path.endswith('.csv'):
            return False
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            if not reader.fieldnames:
                return False
            cols = {c.strip().lower() for c in reader.fieldnames}
            return all(col.lower() in cols for col in required)
    except Exception:
        return False

def load_place_ids_with_city(file_path: str) -> Dict[str, List[str]]:
    """Load mapping of city -> list of place_ids from a CSV with 'place_id' and 'city' columns."""
    city_to_pids: Dict[str, List[str]] = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = (row.get('place_id') or '').strip()
            city = (row.get('city') or '').strip()
            if not pid or not city:
                continue
            city_to_pids.setdefault(city, []).append(pid)
    return city_to_pids

def load_place_ids_with_city_active_window(file_path: str) -> Dict[str, List[str]]:
    """Like load_place_ids_with_city but only include rows where now is within [start_date, end_date] if present.
    Expects columns: place_id, city, start_date, end_date (dates in YYYY-MM-DD or ISO). Empty start/end => always active.
    """
    now = dt.now()
    city_to_pids: Dict[str, List[str]] = {}
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            pid = (row.get('place_id') or '').strip()
            city = (row.get('city') or '').strip()
            if not pid or not city:
                continue
            s = (row.get('start_date') or '').strip()
            e = (row.get('end_date') or '').strip()
            def parse_date(x: str) -> Optional[dt]:
                if not x:
                    return None
                for fmt in ("%Y-%m-%d", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d", "%Y%m%d"):
                    try:
                        return dt.strptime(x, fmt)
                    except Exception:
                        continue
                return None
            sd = parse_date(s)
            ed = parse_date(e)
            active = True
            if sd and now < sd:
                active = False
            if ed and now > ed:
                active = False
            if not active:
                continue
            city_to_pids.setdefault(city, []).append(pid)
    return city_to_pids

def extract_job_name(input_file: str) -> str:
    """Extract job name from input file path (basename without extension)."""
    try:
        # Extract filename from path and remove extension
        filename = os.path.basename(input_file)
        job_name = os.path.splitext(filename)[0]
        
        return job_name
    except Exception as e:
        logger.warning(f"Could not extract job name from {input_file}: {e}")
        return 'unknown_job'

def _sanitize_folder_name(name: str) -> str:
    """Deprecated: no-op sanitizer to preserve legacy flat output structure."""
    return name or ''

def save_json_results(results: List[Dict[str, Any]], output_dir: str, job_name: str) -> None:
    """Save results into a job-specific folder under output_dir with a timestamped file name."""
    try:
        # Create job-specific folder (placeholder) within the output directory
        job_folder = os.path.join(output_dir, job_name if job_name else "results")
        os.makedirs(job_folder, exist_ok=True)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        filename = f"{job_name}_{timestamp}.parquet" if job_name else f"results_{timestamp}.parquet"
        filepath = os.path.join(job_folder, filename)

        json_data = []
        for result in results:
            place_id = result.get('place_id')
            data = result.get('data') or {}
            place_name = result.get('place_name')
            place_address = result.get('place_address')
            place_types = result.get('place_types')
            location_list = result.get('location')
            reviews_count = result.get('reviews_count')
            reviews_distribution = result.get('reviews_distribution')
            utc_now = datetime.now(timezone.utc)
            # Determine local time using timezone hint from data/result first, fallback to coords
            tz_hint = (data or {}).get('timezone') or result.get('timezone')
            tz_name = _infer_timezone_name(location_list, tz_hint, result.get('city'))
            local_iso = _format_local_time_from_utc(utc_now, tz_name)
            json_entry = {
                'placeId': place_id,
                'title': place_name,
                'address': place_address,
                'placeTypes': place_types,
                'location': location_list,
                'reviewsCount': reviews_count,
                'reviewsDistribution': reviews_distribution,
                'currentHour': (data or {}).get('current_hour'),
                'popularTimesLivePercent': (data or {}).get('current_busyness'),
                'popularTimesHistogram': result.get('popular_times_histogram'),
                'openingHours': result.get('opening_hours'),
                'scrapedAtUTC': utc_now.strftime('%Y-%m-%dT%H:%M:%S'),
                'scrapedAtLocal': local_iso,
                'timezone': tz_name
            }
            json_data.append(json_entry)

        try:
            import pyarrow as pa
            import pyarrow.parquet as pq
            table = pa.Table.from_pylist(json_data)
            pq.write_table(table, filepath, compression='zstd')
            logger.info(f"Results saved to Parquet (zstd): {filepath}")
        except Exception as pe:
            # Fallback to JSON if pyarrow not available
            fallback_json = filepath.rsplit('.', 1)[0] + ".json"
            with open(fallback_json, 'w', encoding='utf-8') as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            logger.info(f"PyArrow unavailable; saved JSON instead: {fallback_json}")
    except Exception as e:
        logger.error(f"Error saving JSON results: {e}")
        raise

def save_results(results: List[Dict[str, Any]], output_file: str, input_file: str = None) -> List[str]:
    """Save results grouped by job_id and city into per-job/city folders.
    Returns list of file paths that were created."""
    saved_files = []
    try:
        if not results:
            return saved_files
        base_output_dir = os.path.dirname(output_file)
        # Group by (job_id, city)
        grouped: Dict[tuple, List[Dict[str, Any]]] = {}
        for r in results:
            job_id = (r.get('job_id') or '').strip()
            city = (r.get('city') or '').strip() or 'unknown_city'
            key = (job_id or 'unknown_job', city)
            grouped.setdefault(key, []).append(r)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M")
        for (job_id, city), rows in grouped.items():
            # Sanitize city for folder name: replace any non-word chars (incl. spaces, commas, hyphens) with underscore
            safe_city = re.sub(r'[^\w]+', '_', _sanitize_folder_name(city)).strip('_')
            # Require a valid job_id; skip groups without it to avoid unknown folders
            if not job_id:
                logger.warning("Skipping save for results without job_id")
                continue
            job_dir = os.path.join(
                str(_PROJECT_ROOT / "api_json_extraction" / "uploads"),
                f"job_{job_id}_results",
                safe_city
            )
            os.makedirs(job_dir, exist_ok=True)
            filename = f"{safe_city}_{timestamp}.parquet"
            filepath = os.path.join(job_dir, filename)

            # Convert to prior JSON format
            json_data = []
            for res in rows:
                data = res.get('data') or {}
                loc = res.get('location')
                if isinstance(loc, list) and len(loc) == 2:
                    loc = {'lat': loc[0], 'lng': loc[1]}
                elif not isinstance(loc, dict):
                    loc = {'lat': None, 'lng': None}
                
                # Also check for latitude/longitude from CSV columns
                if isinstance(loc, dict) and not loc.get('lat') and not loc.get('lng'):
                    if res.get('latitude') and res.get('longitude'):
                        loc['lat'] = res.get('latitude')
                        loc['lng'] = res.get('longitude')
                # Compute UTC and local timestamps
                utc_now = datetime.now(timezone.utc)
                # Infer a timezone name (hint -> coords -> UK fallback -> UTC)
                # Check multiple sources for timezone: top-level, data dict, or tz alias
                tz_hint = res.get('timezone') or res.get('tz') or (data or {}).get('timezone') or (data or {}).get('tz')
                tz_name = _infer_timezone_name(loc, tz_hint, res.get('city'))
                local_iso = _format_local_time_from_utc(utc_now, tz_name)
                # Reorder popularTimesHistogram into canonical Monday..Sunday order
                # and keep it as a simple dict: dayName -> [ {hour, occupancyPercent}, ... ].
                # We only include days that actually have data (no null placeholders).
                raw_hist = res.get('popular_times_histogram') or {}
                ordered_hist = {}
                for day_name in ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]:
                    day_hours = raw_hist.get(day_name)
                    if day_hours:
                        ordered_hist[day_name] = day_hours

                json_data.append({
                    'placeId': res.get('place_id'),
                    'title': res.get('place_name'),
                    'address': res.get('place_address'),
                    'placeTypes': res.get('place_types'),
                    'location': loc,
                    'reviewsCount': res.get('reviews_count'),
                    'reviewsDistribution': res.get('reviews_distribution'),
                    'currentHour': (data or {}).get('current_hour'),
                    'popularTimesLivePercent': (data or {}).get('current_busyness'),
                    'popularTimesHistogram': ordered_hist,
                    'openingHours': res.get('opening_hours'),
                    'scrapedAtUTC': utc_now.strftime('%Y-%m-%dT%H:%M:%S'),
                    'scrapedAtLocal': local_iso,
                    'timezone': tz_name
                })
            # Strict requirement: persist hourly results as Parquet only.
            # If Parquet writing fails, raise so the cron logs clearly show the failure.
            try:
                # First try native pyarrow path (fast + zstd compression)
                try:
                    import pyarrow as pa
                    import pyarrow.parquet as pq
                    table = pa.Table.from_pylist(json_data)
                    pq.write_table(table, filepath, compression='zstd')
                    logger.info(f"Saved {len(rows)} results to Parquet (zstd, pyarrow): {filepath}")
                    saved_files.append(filepath)
                except ImportError:
                    # If pyarrow is not installed, fall back to pandas.to_parquet
                    import pandas as pd
                    df = pd.DataFrame(json_data)
                    df.to_parquet(filepath, index=False)
                    logger.info(f"Saved {len(rows)} results to Parquet via pandas.to_parquet: {filepath}")
                    saved_files.append(filepath)
            except Exception as pe:
                logger.error(f"Failed to write Parquet for {len(rows)} results to {filepath}: {pe}")
                # Re-raise so cronjob is visibly failing instead of silently degrading formats
                raise
        return saved_files
    except Exception as e:
        logger.error(f"Error saving results: {e}")
        raise

def _read_csv_rows(file_path: str) -> List[Dict[str, Any]]:
    """Read a CSV as list of dict rows."""
    rows: List[Dict[str, Any]] = []
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            rows.append(row)
    return rows

# --- Opening hours helpers ---

def _parse_time_to_minutes(tok: str):
    if not tok:
        return None
    s = tok.strip().lower()
    if s in ("closed", "-", "n/a"):
        return None
    try:
        if ':' in s and ('am' not in s and 'pm' not in s):
            hh, mm = s.split(':', 1)
            return int(hh) * 60 + int(mm)
        import re
        m = re.match(r"^([0-9]{1,2})(?::([0-9]{2}))?\s*(am|pm)$", s)
        if m:
            hh = int(m.group(1))
            mm = int(m.group(2) or '0')
            mer = m.group(3)
            if hh == 12:
                hh = 0
            if mer == 'pm':
                hh += 12
            return hh * 60 + mm
        if s.isdigit():
            hh = int(s)
            if 0 <= hh <= 24:
                return hh * 60
        return None
    except Exception:
        return None

def _parse_opening_hours(hours_str: str) -> Dict[str, List[tuple]]:
    if not hours_str or not isinstance(hours_str, str):
        return {}
    schedule: Dict[str, List[tuple]] = {}

    # Normalization utilities for diverse inputs
    def normalize_text(s: str) -> str:
        # Replace unicode dashes and narrow no-break spaces, unify whitespace
        s = s.replace('–', '-').replace('—', '-')
        s = s.replace('\u202f', ' ').replace('\xa0', ' ')
        return ' '.join(s.split())

    def is_day_label(s: str) -> bool:
        days = {'monday','tuesday','wednesday','thursday','friday','saturday','sunday'}
        t = s.strip().strip('"').rstrip(':').lower()
        return t in days

    def day_from_label(s: str) -> str:
        t = s.strip().strip('"').rstrip(':')
        return t[0].upper() + t[1:].lower()

    def parse_interval_token(token: str) -> List[tuple]:
        """
        Parse a single interval token which can contain one or multiple ranges
        like '8 am - 12:30 pm, 2-7 pm' and return [(start_min, end_min), ...].
        Handles implicit am/pm by inheriting from end part if missing on start.
        Skips 'Closed'.
        """
        token = normalize_text(token)
        if not token:
            return []
        lt = token.lower()
        if lt == 'closed':
            return []
        if 'open 24 hours' in lt or lt == '24 hours':
            return [(0, 23 * 60 + 59)]
        chunks = [c.strip() for c in token.split(',') if c.strip()]
        out: List[tuple] = []
        for rng in chunks:
            rng = rng.strip().strip(';')
            if not rng or rng.lower() == 'closed':
                continue
            if 'open 24 hours' in rng.lower() or rng.lower() == '24 hours':
                out.append((0, 23 * 60 + 59))
                continue
            if '-' not in rng:
                continue
            start_s, end_s = [t.strip() for t in rng.split('-', 1)]
            # Infer am/pm if missing on start from end; also handle missing on end from start
            lower_start = start_s.lower()
            lower_end = end_s.lower()
            end_suffix = 'am' if lower_end.endswith('am') else ('pm' if lower_end.endswith('pm') else None)
            start_suffix = 'am' if lower_start.endswith('am') else ('pm' if lower_start.endswith('pm') else None)
            s_for_parse = start_s
            e_for_parse = end_s
            if start_suffix is None and end_suffix is not None:
                s_for_parse = f"{start_s} {end_suffix}"
            elif end_suffix is None and start_suffix is not None:
                e_for_parse = f"{end_s} {start_suffix}"
            smin = _parse_time_to_minutes(s_for_parse)
            emin = _parse_time_to_minutes(e_for_parse)
            if smin is None or emin is None:
                continue
            # If the end appears to be before start (e.g., malformed), cap at end of day
            if emin <= smin:
                emin = 23 * 60 + 59
            out.append((smin, emin))
        return out

    text = normalize_text(hours_str)

    # Strategy 1: JSON-like single-line "Day: times; Day: times"
    # Split by semicolons first to catch compact forms
    parts = [p.strip() for p in text.split(';') if p.strip()]
    found_any = False
    for part in parts:
        if ':' in part and is_day_label(part.split(':', 1)[0]):
            day_label, ranges = part.split(':', 1)
            day = day_from_label(day_label)
            if not ranges:
                continue
            intervals = parse_interval_token(ranges)
            if intervals:
                schedule[day] = intervals
                found_any = True

    if found_any:
        return schedule

    # Strategy 2: Line-oriented formats:
    # Day on a line, followed by one or more lines of time ranges, until next Day or end
    lines = [l for l in hours_str.splitlines() if l.strip() != '']
    idx = 0
    while idx < len(lines):
        line = normalize_text(lines[idx])
        if is_day_label(line):
            day = day_from_label(line)
            idx += 1
            day_intervals: List[tuple] = []
            while idx < len(lines):
                next_line = normalize_text(lines[idx])
                if is_day_label(next_line):
                    break
                if next_line:
                    ln = next_line.lower()
                    if ln == 'closed':
                        day_intervals = []
                        idx += 1
                        # skip to next day block
                        while idx < len(lines):
                            probe = normalize_text(lines[idx])
                            if is_day_label(probe):
                                break
                            idx += 1
                        break
                    if 'open 24 hours' in ln or ln == '24 hours':
                        day_intervals = [(0, 23 * 60 + 59)]
                        idx += 1
                        # skip the rest of this day until next day label
                        while idx < len(lines):
                            probe = normalize_text(lines[idx])
                            if is_day_label(probe):
                                break
                            idx += 1
                        break
                    day_intervals.extend(parse_interval_token(next_line))
                idx += 1
            if day_intervals:
                schedule[day] = day_intervals
        else:
            idx += 1

    return schedule

def _is_open_now(hours_str: str, row_data: Dict[str, Any] = None) -> bool:
    """Check if a place is currently open based on opening hours and local timezone.
    
    Args:
        hours_str: Opening hours string (e.g., "Monday: 8 am - 8 pm; Tuesday: 8 am - 8 pm")
        row_data: Row data containing location info for timezone detection
    """
    # Parsing opening-hours strings is CPU-heavy; cache it aggressively.
    sched = _parse_opening_hours_cached(hours_str)
    
    # Determine timezone for this location
    tz_name = None
    
    # Try explicit timezone first
    if row_data:
        tz_name = row_data.get('timezone') or row_data.get('tz')
    
    # If no explicit timezone, try coordinate-based detection (cached)
    if not tz_name and row_data:
        lat = row_data.get('latitude') or row_data.get('lat')
        lng = row_data.get('longitude') or row_data.get('lng')
        if lat and lng:
            try:
                # Round to reduce cache key explosion while being accurate enough.
                key = (round(float(lat), 4), round(float(lng), 4))
                if key in _TZ_CACHE:
                    tz_name = _TZ_CACHE[key]
                else:
                    tz_name = _infer_timezone_name(
                        location={"lat": float(lat), "lng": float(lng)},
                        tz_hint=None,
                    )
                    _TZ_CACHE[key] = tz_name
            except Exception:
                pass
    # Use detected timezone or fallback to UTC
    try:
        if tz_name:
            now = datetime.now(ZoneInfo(tz_name))
        else:
            now = datetime.now(timezone.utc)
    except Exception:
        now = datetime.now(timezone.utc)
    
    weekday = ['Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'][now.weekday()]
    todays = sched.get(weekday)
    if not todays:
        return True
    current_min = now.hour * 60 + now.minute
    for smin, emin in todays:
        if smin <= current_min <= emin:
            return True
    return False

def _csv_has_json_url(file_path: str) -> bool:
    """Return True if CSV has a json_url column (case insensitive match)."""
    with open(file_path, 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            return False
        fields = [c.strip().lower() for c in reader.fieldnames]
        return 'json_url' in fields

def _assign_rows_to_proxies(rows: List[Dict[str, Any]], proxies: List[str]) -> Dict[str, List[Dict[str, Any]]]:
    """Evenly assign rows to proxies in round-robin fashion."""
    assignment: Dict[str, List[Dict[str, Any]]] = {p: [] for p in proxies}
    if not proxies:
        return assignment
    for idx, row in enumerate(rows):
        proxy = proxies[idx % len(proxies)]
        assignment[proxy].append(row)
    return assignment

def _extract_json_response_fields(extractor: LiveBusynessExtractor, data: Dict[str, Any], place_id: str) -> Dict[str, Any]:
    """Extract all fields used in our output JSON from a raw json response."""
    timezone_name = extractor.extract_timezone(data) if hasattr(extractor, 'extract_timezone') else None
    busyness_data = extractor.extract_busyness_data(data)
    # Always ensure timezone is added to busyness_data if it exists
    if busyness_data and isinstance(busyness_data, dict):
        busyness_data = dict(busyness_data)
        if timezone_name:
            busyness_data['timezone'] = timezone_name
    place_name = extractor.extract_place_name(data)
    place_address = extractor.extract_place_address(data)
    # Extract the full list of place types (if available)
    place_types = extractor.extract_place_types(data) if hasattr(extractor, 'extract_place_types') else None
    opening_hours = extractor.extract_opening_hours(data) if hasattr(extractor, 'extract_opening_hours') else None
    popular_times_histogram = extractor.extract_popular_times_histogram(data) if hasattr(extractor, 'extract_popular_times_histogram') else None
    location = extractor.extract_coordinates(data) if hasattr(extractor, 'extract_coordinates') else {'lat': None, 'lng': None}
    if isinstance(location, list) and len(location) == 2:
        location = {'lat': location[0], 'lng': location[1]}
    elif not isinstance(location, dict):
        location = {'lat': None, 'lng': None}
    reviews_count = extractor.extract_reviews_count(data) if hasattr(extractor, 'extract_reviews_count') else None
    reviews_distribution = extractor.extract_reviews_distribution(data) if hasattr(extractor, 'extract_reviews_distribution') else None
    return {
        'place_id': place_id,
        'timestamp': datetime.now().isoformat(),
        'data': busyness_data if busyness_data else {
            'current_hour': None,
            'original_busyness': None,
            'current_busyness': None,
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            **({'timezone': timezone_name} if timezone_name else {})
        },
        'place_name': place_name,
        'place_address': place_address,
        'place_types': place_types,
        'location': location,
        'reviews_count': reviews_count,
        'reviews_distribution': reviews_distribution,
        'opening_hours': opening_hours,
        'popular_times_histogram': popular_times_histogram,
        **({'timezone': timezone_name} if timezone_name else {})
    }


def _has_popular_times(data: Any) -> bool:
    """Check if popular times data exists in the parsed JSON response."""
    try:
        if (
            isinstance(data, list)
            and len(data) > 6
            and isinstance(data[6], list)
            and len(data[6]) > 84
            and data[6][84] is not None
        ):
            return True
    except (IndexError, TypeError):
        return False
    return False

def _warmup_session(session, proxies, delay_range=(0.03, 0.08)):
    """Lightweight warmup: visit Google Maps homepage to establish cookies/CONSENT."""
    try:
        warmup_headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        }
        session.get('https://www.google.com/maps', headers=warmup_headers, proxies=proxies, timeout=15)
        # Set CONSENT cookie explicitly
        session.cookies.set('CONSENT', 'YES+', domain='.google.com')
        session.cookies.set('CONSENT', 'YES+', domain='.google.com.maps')
        # Minimal delay before the actual JSON request (reduced for speed)
        time.sleep(random.uniform(*delay_range))
        return True
    except Exception:
        return False

def _fetch_json_one_stage(session, json_url, proxies):
    """Fetch JSON using a pre-warmed session (1-stage direct fetch)."""
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': '*/*',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'Connection': 'keep-alive',
        'Referer': 'https://www.google.com/maps',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
    }
    try:
        r = session.get(json_url, headers=headers, timeout=20, proxies=proxies)
        r.raise_for_status()
        return r.text, None
    except Exception as e:
        return None, str(e)


def _is_censored(content: str) -> bool:
    """Check if response is censored (data[6][84] is null)."""
    try:
        lines = content.splitlines()
        if lines and lines[0].startswith(")]}'"):
            json_str = "\n".join(lines[1:])
        else:
            json_str = content
        data = json.loads(json_str)
        return data[6][84] is None if len(data) > 6 and len(data[6]) > 84 else True
    except Exception:
        return True

def _parse_preview_response(content: str) -> Optional[Any]:
    """Parse Maps preview response body into a Python object (skips anti-XSSI prefix)."""
    try:
        lines = content.splitlines()
        if lines and lines[0].startswith(")]}'"):
            json_str = "\n".join(lines[1:])
        else:
            json_str = content
        return json.loads(json_str)
    except Exception:
        return None


def _proxy_worker_json_response(
    proxy: str,
    username: str,
    password: str,
    rows: List[Dict[str, Any]],
    delay_range: tuple = (0.01, 0.2),
    cooldown_range: tuple = (30, 60),
    all_proxies: List[str] = None,
    processed_count: List[int] = None,
    progress_lock: threading.Lock = None
) -> List[Dict[str, Any]]:
    """
    Worker that pulls full JSON response for each assigned row.
    Uses 1-stage default (direct JSON fetch with warmup) with retry logic:
    - Try 1-stage direct fetch (with warmup) - up to 2 attempts with different proxies
    - If censored after 2 attempts, fall back to 2-stage
    - If failed (not censored), fall back to 2-stage immediately
    """
    results: List[Dict[str, Any]] = []
    extractor = LiveBusynessExtractor(username, password)
    MAX_ONE_STAGE_ATTEMPTS = 2
    
    # If all_proxies provided, use them for round-robin; otherwise use single proxy
    proxy_list = all_proxies if all_proxies else [proxy]
    proxy_count = len(proxy_list)
    
    # Create persistent sessions per proxy for 1-stage attempts (reuse across rows)
    proxy_sessions: Dict[str, requests.Session] = {}
    
    def _get_proxy_session(proxy_str: str) -> requests.Session:
        """Get or create a persistent session for a proxy."""
        if proxy_str not in proxy_sessions:
            session = requests.Session()
            # Format proxy with credentials
            if proxy_str:
                proxy_target = proxy_str.split("://")[-1]
                auth_proxy = f"http://{username}:{password}@{proxy_target}"
                proxies = {"http": auth_proxy, "https": auth_proxy}
            else:
                proxies = {}
            # Warmup once per proxy session (reduced delay for speed)
            _warmup_session(session, proxies, delay_range=(0.03, 0.08))
            proxy_sessions[proxy_str] = session
        return proxy_sessions[proxy_str]
    
    def _format_proxy_for_requests(proxy_str: str) -> Dict[str, str]:
        """Format proxy string for requests library."""
        if proxy_str:
            proxy_target = proxy_str.split("://")[-1]
            auth_proxy = f"http://{username}:{password}@{proxy_target}"
            return {"http": auth_proxy, "https": auth_proxy}
        return {}
    
    for row in rows:
        place_id = (row.get('place_id') or row.get('PLACE_ID') or row.get('Place ID') or row.get('id') or '').strip()
        json_url = (row.get('json_url') or '').strip()
        city_val = (row.get('city') or '').strip()
        job_id_val = (row.get('job_id') or '').strip()
        if not place_id or not json_url:
            logger.warning(f"Skipping malformed row: missing place_id or json_url")
            continue
        
        successful_result = None
        last_result = None
        tried_proxies = set()
        censored_after_retries = False
        
        # Try 1-stage direct fetch (up to MAX_ONE_STAGE_ATTEMPTS attempts with different proxies)
        for attempt in range(1, MAX_ONE_STAGE_ATTEMPTS + 1):
            # Select proxy (round-robin, avoiding already tried ones)
            available_proxies = [p for p in proxy_list if p not in tried_proxies]
            if not available_proxies:
                # All proxies tried, cycle back
                available_proxies = proxy_list
            current_proxy = available_proxies[(attempt - 1) % len(available_proxies)]
            tried_proxies.add(current_proxy)
            
            proxy_label = current_proxy[:20] if current_proxy else "Localhost"
            logger.debug(f"Place {place_id}: 1-stage attempt {attempt}/{MAX_ONE_STAGE_ATTEMPTS} using proxy: {proxy_label}...")
            
            # Minimal delay (reduced for speed)
            try:
                low, high = delay_range
            except Exception:
                low, high = (0.01, 0.05)  # Further reduced for speed (was 0.01-0.1)
            if high > 0:
                time.sleep(random.uniform(max(0.0, low), high))
            
            try:
                # Get session for this proxy and try 1-stage fetch
                session = _get_proxy_session(current_proxy)
                proxies_dict = _format_proxy_for_requests(current_proxy)
                
                content, error = _fetch_json_one_stage(session, json_url, proxies_dict)
                
                if not content:
                    logger.debug(f"Place {place_id}: 1-stage fetch failed (attempt {attempt}/{MAX_ONE_STAGE_ATTEMPTS}): {error}")
                    continue
                
                # Check if censored
                censored = _is_censored(content)
                parsed = _parse_preview_response(content)
                
                if parsed is None:
                    logger.debug(f"Place {place_id}: Failed to parse JSON response (attempt {attempt}/{MAX_ONE_STAGE_ATTEMPTS})")
                    continue
                
                # Check if popular times data exists
                has_popular_times = False
                try:
                    if (isinstance(parsed, list) and len(parsed) > 6 and 
                        isinstance(parsed[6], list) and len(parsed[6]) > 84 and 
                        parsed[6][84] is not None):
                        has_popular_times = True
                except (IndexError, TypeError):
                    pass
                
                # Process the result
                result_entry = _extract_json_response_fields(extractor, parsed, place_id)
                if city_val:
                    result_entry['city'] = city_val
                if job_id_val:
                    result_entry['job_id'] = job_id_val
                
                last_result = {
                    "success": True,
                    "place_id": place_id,
                    "data": parsed,
                    "result_entry": result_entry,
                    "has_popular_times": has_popular_times,
                    "censored": censored
                }
                
                # If we got popular times data, we're done
                if has_popular_times:
                    logger.info(f"Place {place_id}: Successfully got popular times data via 1-stage (attempt {attempt}/{MAX_ONE_STAGE_ATTEMPTS})")
                    successful_result = last_result
                    break
                else:
                    # Censored response - log and continue to next proxy/attempt
                    logger.debug(f"Place {place_id}: Got censored response via 1-stage (attempt {attempt}/{MAX_ONE_STAGE_ATTEMPTS}), trying next...")
                    censored_after_retries = True
                    continue
                
            except Exception as e:
                logger.error(f"Error processing {place_id} via 1-stage (attempt {attempt}/{MAX_ONE_STAGE_ATTEMPTS}): {e}")
                last_result = {
                    "success": False,
                    "error": str(e),
                    "place_id": place_id,
                    "censored": None
                }
                continue
        
        # If 1-stage failed or censored after retries, try 2-stage fallback
        if not successful_result:
            if censored_after_retries or (last_result and last_result.get("censored")):
                # Censored after retries - try 2-stage fallback
                logger.debug(f"Place {place_id}: Trying 2-stage fallback after {MAX_ONE_STAGE_ATTEMPTS} censored 1-stage attempts")
            else:
                # Failed (not censored) - try 2-stage fallback immediately
                logger.debug(f"Place {place_id}: Trying 2-stage fallback after 1-stage failure")
            
            # Try 2-stage with the first proxy
            two_stage_proxy = proxy_list[0]
            try:
                time.sleep(random.uniform(0.05, 0.1))  # Further reduced delay before 2-stage (was 0.1-0.2)
                data = extractor.get_json_data(json_url, two_stage_proxy, place_id)
                
                if data:
                    # Check if popular times data exists
                    has_popular_times = False
                    try:
                        if (isinstance(data, list) and len(data) > 6 and 
                            isinstance(data[6], list) and len(data[6]) > 84 and 
                            data[6][84] is not None):
                            has_popular_times = True
                    except (IndexError, TypeError):
                        pass
                    
                    # Process the result
                    result_entry = _extract_json_response_fields(extractor, data, place_id)
                    if city_val:
                        result_entry['city'] = city_val
                    if job_id_val:
                        result_entry['job_id'] = job_id_val
                    
                    last_result = {
                        "success": True,
                        "place_id": place_id,
                        "data": data,
                        "result_entry": result_entry,
                        "has_popular_times": has_popular_times,
                        "censored": not has_popular_times
                    }
                    
                    if has_popular_times:
                        logger.info(f"Place {place_id}: Successfully got popular times data via 2-stage fallback")
                        successful_result = last_result
                    else:
                        logger.warning(f"Place {place_id}: 2-stage fallback returned censored data")
                else:
                    logger.warning(f"Place {place_id}: 2-stage fallback returned no data")
            except Exception as e:
                logger.error(f"Error processing {place_id} via 2-stage fallback: {e}")
        
        # Save result
        if successful_result:
            results.append(successful_result["result_entry"])
        elif last_result and last_result.get("success"):
            # Save even if censored (we got data, just missing popular times)
            results.append(last_result["result_entry"])
            logger.warning(f"Place {place_id}: Saved result but popular times data is missing")
        else:
            # Complete failure - log but don't save
            logger.warning(f"Place {place_id}: Failed after all attempts. Error: {last_result.get('error', 'Unknown') if last_result else 'No data returned'}")
        
        # Update progress counter incrementally (thread-safe)
        if processed_count is not None and progress_lock is not None:
            with progress_lock:
                processed_count[0] += 1
        
        # Minimal delay after request (reduced for speed)
        time.sleep(random.uniform(0.01, 0.03))  # Further reduced for speed (was 0.02-0.08)
    
    # Close all sessions
    for session in proxy_sessions.values():
        session.close()
    
    return results

def process_place_ids(
    place_ids: List[str],
    extractor: LiveBusynessExtractor,
    max_retries: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0
) -> List[Dict[str, Any]]:
    """Process place IDs with improved retry logic."""
    results = []
    failed_ids: Set[str] = set()
    processed_ids: Set[str] = set()
    total_place_ids = len(place_ids)
    
    logger.info(f"Starting to process {total_place_ids} place IDs")
    
    # First pass: Try to process all place IDs
    for i, place_id in enumerate(place_ids, 1):
        if place_id in processed_ids:
            continue
            
        logger.info(f"Processing place_id {i}/{total_place_ids}: {place_id}")
        success = False
        
        for attempt in range(max_retries):
            try:
                proxy = extractor.get_next_proxy(place_id)
                json_url = extractor.get_json_url(place_id, proxy)
                
                if not json_url:
                    logger.warning(f"Failed to get JSON URL for place_id {place_id} (attempt {attempt + 1})")
                    time.sleep(min(base_delay * (2 ** attempt), max_delay))
                    continue
                
                json_data = extractor.get_json_data(json_url, proxy, place_id)
                if not json_data:
                    logger.warning(f"Failed to get JSON data for place_id {place_id} (attempt {attempt + 1})")
                    time.sleep(min(base_delay * (2 ** attempt), max_delay))
                    continue
                
                timezone_name = extractor.extract_timezone(json_data) if hasattr(extractor, 'extract_timezone') else None
                busyness_data = extractor.extract_busyness_data(json_data)
                if busyness_data and timezone_name and isinstance(busyness_data, dict):
                    busyness_data = dict(busyness_data)
                    busyness_data['timezone'] = timezone_name
                if busyness_data:
                    place_name = extractor.extract_place_name(json_data)
                    place_address = extractor.extract_place_address(json_data)
                    place_types = extractor.extract_place_types(json_data) if hasattr(extractor, 'extract_place_types') else None
                    location_obj = extractor.extract_coordinates(json_data) if hasattr(extractor, 'extract_coordinates') else {'lat': None, 'lng': None}
                    if isinstance(location_obj, list) and len(location_obj) == 2:
                        location_obj = {'lat': location_obj[0], 'lng': location_obj[1]}
                    elif not isinstance(location_obj, dict):
                        location_obj = {'lat': None, 'lng': None}
                    reviews_count = extractor.extract_reviews_count(json_data) if hasattr(extractor, 'extract_reviews_count') else None
                    reviews_distribution = extractor.extract_reviews_distribution(json_data) if hasattr(extractor, 'extract_reviews_distribution') else None
                    opening_hours = extractor.extract_opening_hours(json_data) if hasattr(extractor, 'extract_opening_hours') else None
                    popular_times_histogram = extractor.extract_popular_times_histogram(json_data) if hasattr(extractor, 'extract_popular_times_histogram') else None
                    results.append({
                        'place_id': place_id,
                        'timestamp': datetime.now().isoformat(),
                        'data': busyness_data,
                        'place_name': place_name,
                        'place_address': place_address,
                        'place_types': place_types,
                        'location': location_obj,
                        'reviews_count': reviews_count,
                        'reviews_distribution': reviews_distribution,
                        'opening_hours': opening_hours,
                        'popular_times_histogram': popular_times_histogram,
                        **({'timezone': timezone_name} if timezone_name else {})
                    })
                    processed_ids.add(place_id)
                    success = True
                    logger.info(f"Successfully processed place_id {place_id}")
                    break
                else:
                    logger.warning(f"No busyness data found for place_id {place_id} (attempt {attempt + 1})")
                    time.sleep(min(base_delay * (2 ** attempt), max_delay))
                    
            except Exception as e:
                logger.error(f"Error processing place_id {place_id} (attempt {attempt + 1}): {e}")
                time.sleep(min(base_delay * (2 ** attempt), max_delay))
        
        if not success:
            failed_ids.add(place_id)
            logger.error(f"Failed to process place_id {place_id} after {max_retries} attempts, will retry later")
    
    # Second pass: Retry failed IDs with longer delays
    if failed_ids:
        logger.info(f"Starting second pass to retry {len(failed_ids)} failed place IDs...")
        still_failed = set()
        
        for i, place_id in enumerate(failed_ids, 1):
            logger.info(f"Retrying place_id {i}/{len(failed_ids)}: {place_id}")
            success = False
            
            for attempt in range(max_retries):
                try:
                    proxy = extractor.get_next_proxy(place_id)
                    json_url = extractor.get_json_url(place_id, proxy)
                    
                    if not json_url:
                        logger.warning(f"Failed to get JSON URL for place_id {place_id} (retry attempt {attempt + 1})")
                        time.sleep(min(base_delay * (2 ** (attempt + max_retries)), max_delay))
                        continue
                    
                    json_data = extractor.get_json_data(json_url, proxy, place_id)
                    if not json_data:
                        logger.warning(f"Failed to get JSON data for place_id {place_id} (retry attempt {attempt + 1})")
                        time.sleep(min(base_delay * (2 ** (attempt + max_retries)), max_delay))
                        continue
                    timezone_name = extractor.extract_timezone(json_data) if hasattr(extractor, 'extract_timezone') else None
                    busyness_data = extractor.extract_busyness_data(json_data)
                    if busyness_data and timezone_name and isinstance(busyness_data, dict):
                        busyness_data = dict(busyness_data)
                        busyness_data['timezone'] = timezone_name
                    if busyness_data:
                        place_name = extractor.extract_place_name(json_data)
                        place_address = extractor.extract_place_address(json_data)
                        location_list = extractor.extract_coordinates(json_data) if hasattr(extractor, 'extract_coordinates') else [None, None]
                        reviews_count = extractor.extract_reviews_count(json_data) if hasattr(extractor, 'extract_reviews_count') else None
                        reviews_distribution = extractor.extract_reviews_distribution(json_data) if hasattr(extractor, 'extract_reviews_distribution') else None
                        opening_hours = extractor.extract_opening_hours(json_data) if hasattr(extractor, 'extract_opening_hours') else None
                        popular_times_histogram = extractor.extract_popular_times_histogram(json_data) if hasattr(extractor, 'extract_popular_times_histogram') else None
                        results.append({
                            'place_id': place_id,
                            'timestamp': datetime.now().isoformat(),
                            'data': busyness_data,
                            'place_name': place_name,
                            'place_address': place_address,
                            'location': location_list,
                            'reviews_count': reviews_count,
                            'reviews_distribution': reviews_distribution,
                            'opening_hours': opening_hours,
                            'popular_times_histogram': popular_times_histogram,
                            **({'timezone': timezone_name} if timezone_name else {})
                        })
                        processed_ids.add(place_id)
                        success = True
                        logger.info(f"Successfully processed place_id {place_id} on retry")
                        break
                    else:
                        logger.warning(f"No busyness data found for place_id {place_id} (retry attempt {attempt + 1})")
                        time.sleep(min(base_delay * (2 ** (attempt + max_retries)), max_delay))
                    
                except Exception as e:
                    logger.error(f"Error processing place_id {place_id} (retry attempt {attempt + 1}): {e}")
                    time.sleep(min(base_delay * (2 ** (attempt + max_retries)), max_delay))
            
            if not success:
                still_failed.add(place_id)
                logger.error(f"Failed to process place_id {place_id} after {max_retries} retry attempts")
        
        # Log final summary
        logger.info(f"Processing complete. Successfully processed {len(processed_ids)}/{total_place_ids} place IDs")
        if still_failed:
            logger.error(f"Failed to process {len(still_failed)} place IDs after all retries: {still_failed}")
    
    return results

def _process_single_place_id(
    place_id: str,
    extractor: LiveBusynessExtractor,
    max_retries: int,
    base_delay: float,
    max_delay: float
) -> Optional[Dict[str, Any]]:
    """Worker-safe single place_id processing with retries. Returns result dict or None."""
    for attempt in range(max_retries):
        try:
            proxy = extractor.get_next_proxy(place_id)
            json_url = extractor.get_json_url(place_id, proxy)
            if not json_url:
                time.sleep(min(base_delay * (2 ** attempt), max_delay))
                continue

            json_data = extractor.get_json_data(json_url, proxy, place_id)
            if not json_data:
                time.sleep(min(base_delay * (2 ** attempt), max_delay))
                continue

            timezone_name = extractor.extract_timezone(json_data) if hasattr(extractor, 'extract_timezone') else None
            busyness_data = extractor.extract_busyness_data(json_data)
            if busyness_data and timezone_name and isinstance(busyness_data, dict):
                busyness_data = dict(busyness_data)
                busyness_data['timezone'] = timezone_name
            if busyness_data:
                place_name = extractor.extract_place_name(json_data)
                place_address = extractor.extract_place_address(json_data)
                place_types = extractor.extract_place_types(json_data) if hasattr(extractor, 'extract_place_types') else None
                location_list = extractor.extract_coordinates(json_data) if hasattr(extractor, 'extract_coordinates') else [None, None]
                reviews_count = extractor.extract_reviews_count(json_data) if hasattr(extractor, 'extract_reviews_count') else None
                reviews_distribution = extractor.extract_reviews_distribution(json_data) if hasattr(extractor, 'extract_reviews_distribution') else None
                opening_hours = extractor.extract_opening_hours(json_data) if hasattr(extractor, 'extract_opening_hours') else None
                popular_times_histogram = extractor.extract_popular_times_histogram(json_data) if hasattr(extractor, 'extract_popular_times_histogram') else None
                return {
                    'place_id': place_id,
                    'timestamp': datetime.now().isoformat(),
                    'data': busyness_data,
                    'place_name': place_name,
                    'place_address': place_address,
                    'place_types': place_types,
                    'location': location_list,
                    'reviews_count': reviews_count,
                    'reviews_distribution': reviews_distribution,
                    'opening_hours': opening_hours,
                    'popular_times_histogram': popular_times_histogram,
                    **({'timezone': timezone_name} if timezone_name else {})
                }
        except Exception:
            time.sleep(min(base_delay * (2 ** attempt), max_delay))
            continue
    return None

def process_place_ids_concurrent(
    place_ids: List[str],
    extractor: LiveBusynessExtractor,
    num_workers: int,
    max_retries: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0
) -> List[Dict[str, Any]]:
    """Process place IDs concurrently using a pool sized to available proxies."""
    results: List[Dict[str, Any]] = []
    total = len(place_ids)
    logger.info(f"Processing {total} place IDs with {num_workers} workers")
    with ThreadPoolExecutor(max_workers=max(1, num_workers)) as executor:
        future_to_pid = {
            executor.submit(
                _process_single_place_id,
                pid,
                extractor,
                max_retries,
                base_delay,
                max_delay,
            ): pid for pid in place_ids
        }
        completed = 0
        for future in as_completed(future_to_pid):
            pid = future_to_pid[future]
            try:
                res = future.result()
                if res is not None:
                    results.append(res)
            except Exception as e:
                logger.error(f"Worker error for place_id {pid}: {e}")
            completed += 1
            if completed % 50 == 0 or completed == total:
                logger.info(f"Progress: {completed}/{total} place IDs processed")
    return results

def process_place_ids_rotate_proxies(
    place_ids: List[str],
    extractor: LiveBusynessExtractor,
    proxies: List[str],
    max_retries: int = 3,
    base_delay: float = 2.0,
    max_delay: float = 30.0
) -> List[Dict[str, Any]]:
    """Process place IDs sequentially, immediately switching to the next proxy on failure."""
    results: List[Dict[str, Any]] = []
    total = len(place_ids)
    for idx, place_id in enumerate(place_ids, 1):
        logger.info(f"[{idx}/{total}] place_id={place_id}")
        success = False
        for attempt in range(max_retries):
            for px in proxies:
                try:
                    json_url = extractor.get_json_url(place_id, px)
                    if not json_url:
                        continue
                    json_data = extractor.get_json_data(json_url, px, place_id)
                    if not json_data:
                        continue
                    timezone_name = extractor.extract_timezone(json_data) if hasattr(extractor, 'extract_timezone') else None
                    busyness_data = extractor.extract_busyness_data(json_data)
                    if busyness_data and timezone_name and isinstance(busyness_data, dict):
                        busyness_data = dict(busyness_data)
                        busyness_data['timezone'] = timezone_name
                    if not busyness_data:
                        continue
                    place_name = extractor.extract_place_name(json_data)
                    place_address = extractor.extract_place_address(json_data)
                    place_types = extractor.extract_place_types(json_data) if hasattr(extractor, 'extract_place_types') else None
                    location_list = extractor.extract_coordinates(json_data) if hasattr(extractor, 'extract_coordinates') else [None, None]
                    reviews_count = extractor.extract_reviews_count(json_data) if hasattr(extractor, 'extract_reviews_count') else None
                    reviews_distribution = extractor.extract_reviews_distribution(json_data) if hasattr(extractor, 'extract_reviews_distribution') else None
                    opening_hours = extractor.extract_opening_hours(json_data) if hasattr(extractor, 'extract_opening_hours') else None
                    popular_times_histogram = extractor.extract_popular_times_histogram(json_data) if hasattr(extractor, 'extract_popular_times_histogram') else None
                    results.append({
                        'place_id': place_id,
                        'timestamp': datetime.now().isoformat(),
                        'data': busyness_data,
                        'place_name': place_name,
                        'place_address': place_address,
                        'place_types': place_types,
                        'location': location_list,
                        'reviews_count': reviews_count,
                        'reviews_distribution': reviews_distribution,
                        'opening_hours': opening_hours,
                        'popular_times_histogram': popular_times_histogram,
                        **({'timezone': timezone_name} if timezone_name else {})
                    })
                    success = True
                    break
                except Exception as e:
                    logger.warning(f"Proxy {px} failed for {place_id}: {e}")
                    continue
            if success:
                break
            time.sleep(min(base_delay * (2 ** attempt), max_delay))
        if not success:
            logger.warning(f"Giving up on place_id {place_id} after {max_retries} attempts across proxies")
    return results

def main():
    """Main function to run the busyness extraction."""
    # Enforce single-instance execution across cron/manual runs for this repo
    if not _acquire_single_instance_lock(LOCK_FILE):
        logger.warning(
            f"Another instance of live_busyness_extraction is already running. "
            f"Exiting to prevent duplicate runs. @api_json_extraction @live_busyness_extraction"
        )
        return

    try:
        # Set up argument parser
        parser = argparse.ArgumentParser(description='Extract live busyness data from Google Maps')
        parser.add_argument('--proxy_username', required=True, help='Proxy username')
        parser.add_argument('--proxy_password', required=True, help='Proxy password')
        parser.add_argument('--input_file', required=True, help='Input file containing place IDs')
        parser.add_argument('--output_file', required=True, help='Output file to save results')
        parser.add_argument('--single_proxy', required=False, help='Use only this proxy (host:port) for all requests')
        # Default to 4 workers per proxy to better utilize large proxy pools (e.g., 110 proxies)
        parser.add_argument(
            '--per_proxy_concurrency',
            type=int,
            default=10,
            help='Number of concurrent workers per proxy (JSON-response pipeline). Default: 10 for maximum throughput (target: 400k places in 60 minutes).'
        )
        
        args = parser.parse_args()

        # Get the latest proxy file
        proxy_file = get_latest_proxy_file()
        logger.info(f"Using proxy file: {proxy_file}")
        
        # Initialize extractor
        extractor = LiveBusynessExtractor(args.proxy_username, args.proxy_password)
        extractor.load_proxies(proxy_file)
        
        # Decide pipeline based on input file type/columns
        is_csv_with_json_url = False
        if args.input_file.endswith('.csv'):
            try:
                is_csv_with_json_url = _csv_has_json_url(args.input_file)
            except Exception:
                is_csv_with_json_url = False
        is_csv_with_city = csv_has_columns(args.input_file, ['place_id', 'city'])
        is_csv_with_city_window = csv_has_columns(args.input_file, ['place_id', 'city', 'start_date', 'end_date'])

        # Build proxy list to use (single provided => prioritized fallback)
        if not extractor.proxies:
            raise RuntimeError("No proxies loaded; cannot determine number of workers")
        if args.single_proxy:
            proxies_to_use = [args.single_proxy] + [p for p in extractor.proxies if p != args.single_proxy]
        else:
            proxies_to_use = extractor.proxies
        logger.info(f"Using {len(proxies_to_use)} proxies (first: {proxies_to_use[0]})")

        # If CSV has json_url column, run parallel JSON-response pipeline (max_workers = number of proxies)
        if is_csv_with_json_url:
            logger.info("Detected json_url column in input CSV. Running parallel JSON-response pipeline...")
            rows = _read_csv_rows(args.input_file)
            # Keep only rows with a valid job_id to ensure proper per-job saving
            rows = [r for r in rows if (r.get('job_id') or '').strip()]
            
            # Filter by opening hours on the fly - only process places that are currently open
            # If opening_hours is empty/missing, include the row (can't determine, so process it)
            filtered_rows: List[Dict[str, Any]] = []
            for r in rows:
                oh = r.get('opening_hours') or r.get('openingHours') or r.get('Opening Hours')
                if not oh or not str(oh).strip():
                    # No opening hours info - include it (process it)
                    filtered_rows.append(r)
                elif _is_open_now(str(oh), r):
                    # Has opening hours and is currently open - include it
                    filtered_rows.append(r)
                # else: has opening hours but is closed - skip it
            
            if len(filtered_rows) != len(rows):
                logger.info(f"Opening-hours filter: keeping {len(filtered_rows)}/{len(rows)} rows (currently open or no hours data)")
            else:
                logger.info(f"All {len(rows)} rows will be processed")
            # Explicit cron-friendly summary for downstream log filtering
            logger.info(
                f"Working for {len(filtered_rows)} open place ids out of {len(rows)} total place ids "
                f"in the cumulative input file @api_json_extraction @live_busyness_extraction"
            )
            
            rows = filtered_rows
            logger.info(f"Processing {len(rows)} place_id row(s) for JSON-response pipeline")

            # Build job_id -> place_ids mapping for per-job saving
            job_id_to_place_ids: Dict[str, set] = {}
            place_id_to_job_id: Dict[str, str] = {}
            for r in rows:
                job_id = (r.get('job_id') or '').strip()
                place_id = (r.get('place_id') or r.get('PLACE_ID') or r.get('Place ID') or '').strip()
                if job_id and place_id:
                    job_id_to_place_ids.setdefault(job_id, set()).add(place_id)
                    place_id_to_job_id[place_id] = job_id

            logger.info(f"Found {len(job_id_to_place_ids)} unique job_ids in input")

            # Use ThreadPoolExecutor-based JSON-response pipeline
            # Synchronous ThreadPoolExecutor-based pipeline (existing behavior)
            # evenly assign rows to proxies
            assignment = _assign_rows_to_proxies(rows, proxies_to_use)
            per_proxy = max(1, int(args.per_proxy_concurrency))
            max_workers = len(proxies_to_use) * per_proxy
            all_results: List[Dict[str, Any]] = []

            # Thread-safe structures for incremental saving
            progress_lock = threading.Lock()
            processed_count = [0]  # Use list to allow modification from nested functions
            total_count = len(rows)
            # Track completed place_ids per job_id (thread-safe)
            job_results: Dict[str, List[Dict[str, Any]]] = {}  # job_id -> list of results
            job_completed_place_ids: Dict[str, set] = {}  # job_id -> set of completed place_ids
            job_saved: Dict[str, bool] = {}  # Track which jobs have been saved
            for job_id in job_id_to_place_ids:
                job_results[job_id] = []
                job_completed_place_ids[job_id] = set()
                job_saved[job_id] = False

            def log_progress_periodically():
                """Background thread that logs progress every 2 minutes."""
                while True:
                    time.sleep(120)  # 2 minutes
                    with progress_lock:
                        current = processed_count[0]
                    if current < total_count:
                        pct = (current / total_count * 100) if total_count > 0 else 0
                        logger.info(
                            f"Progress: processed {current} out of {total_count} open place ids ({pct:.1f}%) "
                            f"@api_json_extraction @live_busyness_extraction"
                        )
                    else:
                        # All done, exit thread
                        break

            # Start progress logging thread
            progress_thread = threading.Thread(target=log_progress_periodically, daemon=True)
            progress_thread.start()

            def save_job_if_complete(job_id: str):
                """
                Check if job is complete and save if so.
                Must be called with progress_lock held. Prepares data and marks as saved,
                then saves outside the lock to avoid blocking other threads.
                Returns True if job was saved, False otherwise.
                """
                if job_saved.get(job_id, False):
                    return False  # Already saved

                expected_place_ids = job_id_to_place_ids.get(job_id, set())
                completed_place_ids = job_completed_place_ids.get(job_id, set())

                # Check if all place_ids for this job are completed
                if expected_place_ids.issubset(completed_place_ids):
                    job_results_list = job_results.get(job_id, [])
                    if job_results_list:
                        # Convert to save format (still holding lock)
                        results_for_save = []
                        for r in job_results_list:
                            loc = r.get('location')
                            if isinstance(loc, list) and len(loc) == 2:
                                loc = {'lat': loc[0], 'lng': loc[1]}
                            elif not isinstance(loc, dict):
                                loc = {'lat': None, 'lng': None}
                            results_for_save.append({
                                'place_id': r.get('place_id'),
                                'timestamp': r.get('timestamp'),
                                'data': r.get('data'),
                                'place_name': r.get('place_name'),
                                'place_address': r.get('place_address'),
                                'place_types': r.get('place_types'),
                                'location': loc,
                                'reviews_count': r.get('reviews_count'),
                                'reviews_distribution': r.get('reviews_distribution'),
                                'opening_hours': r.get('opening_hours'),
                                'popular_times_histogram': r.get('popular_times_histogram'),
                                'city': r.get('city'),
                                'timezone': r.get('timezone') or ((r.get('data') or {}).get('timezone')),
                                'job_id': job_id
                            })

                        # Mark as saved before releasing lock
                        job_saved[job_id] = True

                        # Return data to save outside the lock
                        return results_for_save
                return None

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for px in proxies_to_use:
                        px_rows = assignment.get(px, [])
                        if not px_rows:
                            continue
                        # Split each proxy's assigned rows across multiple workers to increase throughput.
                        # Each worker still rotates across all proxies on retry (to escape censoring).
                        chunk_size = max(1, (len(px_rows) + per_proxy - 1) // per_proxy)
                        for i in range(0, len(px_rows), chunk_size):
                            chunk = px_rows[i:i + chunk_size]
                            futures.append(
                                executor.submit(
                                    _proxy_worker_json_response,
                                    px,  # primary proxy for this worker
                                    args.proxy_username,
                                    args.proxy_password,
                                    chunk,
                                    (0.01, 0.05),  # further reduced jitter for speed (was 0.01-0.1)
                                    (0.0, 0.0),  # no per-place cooldown sleep
                                    proxies_to_use,  # pass all proxies for round-robin cycling
                                    processed_count,  # pass progress counter for incremental updates
                                    progress_lock,  # pass lock for thread-safe updates
                                )
                            )
                for fut in as_completed(futures):
                        try:
                            res = fut.result()
                            if res:
                                all_results.extend(res)

                                # Group results by job_id and check for completion
                                jobs_to_save = {}  # job_id -> results_for_save
                                with progress_lock:
                                    for result_entry in res:
                                        place_id = result_entry.get('place_id')
                                        job_id = place_id_to_job_id.get(place_id) if place_id else None

                                        if job_id:
                                            # Add to job's results
                                            job_results.setdefault(job_id, []).append(result_entry)
                                            # Mark place_id as completed
                                            job_completed_place_ids.setdefault(job_id, set()).add(place_id)

                                            # Check if this job is now complete (returns data to save or None)
                                            results_to_save = save_job_if_complete(job_id)
                                            if results_to_save:
                                                jobs_to_save[job_id] = results_to_save

                                # Save completed jobs outside the lock (to avoid blocking)
                                for job_id, results_for_save in jobs_to_save.items():
                                    try:
                                        saved_files = save_results(results_for_save, args.output_file, args.input_file)
                                        # Log summary with parquet file names
                                        if saved_files:
                                            file_names = [os.path.basename(f) for f in saved_files]
                                            logger.info(
                                                f"job_{job_id}_results: Saved {len(results_for_save)} results to {len(saved_files)} parquet file(s): {', '.join(file_names)} "
                                                f"@api_json_extraction @live_busyness_extraction"
                                            )
                                        else:
                                            logger.warning(f"job_{job_id}_results: No files were saved (empty results?)")
                                    except Exception as e:
                                        logger.error(f"Failed to incrementally save job_id {job_id}: {e}")
                                        # Reset saved flag on error so we can retry later
                                        with progress_lock:
                                            job_saved[job_id] = False

                                # Note: progress counter is updated incrementally inside _proxy_worker_json_response
                                # as each row is processed, so we don't need to update it here
                        except Exception as e:
                            logger.error(f"Proxy worker error: {e}")

            # Save any remaining incomplete jobs (in case some place_ids failed)
            with progress_lock:
                    for job_id in job_id_to_place_ids:
                        if not job_saved.get(job_id, False):
                            job_results_list = job_results.get(job_id, [])
                            if job_results_list:
                                # Convert to save format
                                results_for_save = []
                                for r in job_results_list:
                                    loc = r.get('location')
                                    if isinstance(loc, list) and len(loc) == 2:
                                        loc = {'lat': loc[0], 'lng': loc[1]}
                                    elif not isinstance(loc, dict):
                                        loc = {'lat': None, 'lng': None}
                                    results_for_save.append({
                                        'place_id': r.get('place_id'),
                                        'timestamp': r.get('timestamp'),
                                        'data': r.get('data'),
                                        'place_name': r.get('place_name'),
                                        'place_address': r.get('place_address'),
                                        'place_types': r.get('place_types'),
                                        'location': loc,
                                        'reviews_count': r.get('reviews_count'),
                                        'reviews_distribution': r.get('reviews_distribution'),
                                        'opening_hours': r.get('opening_hours'),
                                        'popular_times_histogram': r.get('popular_times_histogram'),
                                        'city': r.get('city'),
                                        'timezone': r.get('timezone') or ((r.get('data') or {}).get('timezone')),
                                        'job_id': job_id
                                    })

                                # Save remaining results for this job
                                try:
                                    saved_files = save_results(results_for_save, args.output_file, args.input_file)
                                    # Log summary with parquet file names
                                    if saved_files:
                                        file_names = [os.path.basename(f) for f in saved_files]
                                        logger.info(
                                            f"job_{job_id}_results: Saved {len(results_for_save)} results to {len(saved_files)} parquet file(s): {', '.join(file_names)} "
                                            f"@api_json_extraction @live_busyness_extraction"
                                        )
                                    else:
                                        logger.warning(f"job_{job_id}_results: No files were saved (empty results?)")
                                except Exception as e:
                                    logger.error(f"Failed to save remaining results for job_id {job_id}: {e}")

            logger.info(f"All processing complete. Processed {len(all_results)} total results.")

        else:
            # CSV with city column: process per-city and save into per-city output folders
            if is_csv_with_city or is_csv_with_city_window:
                city_map = load_place_ids_with_city_active_window(args.input_file) if is_csv_with_city_window else load_place_ids_with_city(args.input_file)
                output_dir = os.path.dirname(args.output_file)
                total_cities = len(city_map)
                logger.info(f"Detected city column. Processing {total_cities} cities...")
                for idx, (city, place_ids) in enumerate(city_map.items(), 1):
                    logger.info(f"[{idx}/{total_cities}] City '{city}' with {len(place_ids)} place IDs")
                    if not place_ids:
                        continue
                    results = process_place_ids_rotate_proxies(place_ids, extractor, proxies=proxies_to_use)
                    # Save results into city-specific folder using existing helper
                    try:
                        save_json_results(results, output_dir, job_name=city)
                    except Exception as e:
                        logger.error(f"Failed saving results for city {city}: {e}")
                logger.info("Per-city processing complete.")
            else:
                # Fallback: original pipeline using place_ids list
                place_ids = load_place_ids(args.input_file)
                logger.info(f"Loaded {len(place_ids)} place IDs")
                job_name = extract_job_name(args.input_file)
                logger.info(f"Job name: {job_name}")
                logger.info("Running sequential pipeline with immediate proxy fallback per place_id...")
                results = process_place_ids_rotate_proxies(place_ids, extractor, proxies=proxies_to_use)
                save_results(results, args.output_file, args.input_file)
        
    except Exception as e:
        logger.error(f"Error in main: {e}")
        raise
    finally:
        _release_single_instance_lock(LOCK_FILE)

if __name__ == "__main__":
    main() 