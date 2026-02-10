import os
import json
from datetime import datetime
import shortuuid
import pandas as pd
from app.services.json_extractor import get_json_results, DEFAULT_PROXY_USERNAME, DEFAULT_PROXY_PASSWORD
from app.services.proxy_manager import get_proxy, release_proxy
import time, csv
import logging, shutil, zipfile
import glob
import numpy as np
from db.database import database
from db.models import jobs, users
import redis
from config.config import Config
from typing import List, Dict, Any, Tuple
from crontab import CronTab
import sys 
import traceback
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

# Resolve project paths relative to this file so everything works in any checkout directory
_THIS_FILE = Path(__file__).resolve()
_APP_DIR = _THIS_FILE.parents[1]                 # api_json_extraction/app
_API_ROOT = _APP_DIR.parent                      # api_json_extraction
_PROJECT_ROOT = _API_ROOT.parent                 # repository root

# Ensure project root is on sys.path so we can import sibling top-level packages
for _p in (_API_ROOT, _PROJECT_ROOT):
    _p_str = str(_p)
    if _p_str not in sys.path:
        sys.path.append(_p_str)

from live_busyness_extraction.extractor import LiveBusynessExtractor
from live_busyness_extraction.main import get_latest_proxy_file

# Set up file-based logging inside this repo
log_dir = os.path.join(str(_API_ROOT), "logs")
os.makedirs(log_dir, exist_ok=True)

# Configure logging to write to both file and console
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(log_dir, 'job_manager.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

DELAY_RANGE = (0.01, 0.2) 
COOLDOWN_RANGE = (30, 60)  

def assign_place_ids_to_proxies(place_ids: List[str], place_types: List[str], proxies: List[str]) -> Dict[str, List[Tuple[str, str]]]:
    """Assign place_ids and place_types to proxies in round-robin fashion."""
    assignment = {proxy: [] for proxy in proxies}
    for idx, (place_id, place_type) in enumerate(zip(place_ids, place_types)):
        proxy = proxies[idx % len(proxies)]
        assignment[proxy].append((place_id, place_type))
    return assignment

def is_blocked_exception(e: Exception) -> bool:
    """Heuristic to determine if an exception indicates proxy blocking."""
    # Treat all exceptions as potential blocks for cooldown
    return True

def process_place_id_worker(
    proxy: str,
    username: str,
    password: str,
    place_id_type_pairs: List[Tuple[str, str]],
    delay_range: Tuple[float, float],
    cooldown_range: Tuple[float, float],
    progress_counter: Dict[str, int],
    progress_lock: threading.Lock,
    total_place_ids: int
) -> List[Tuple[str, str, str, Dict[str, Any], bool]]:
    """
    Worker function to process place IDs assigned to a specific proxy.
    Uses the assigned proxy exclusively for its assigned place IDs with delays and cooldowns.
    
    Returns:
        List of tuples: (place_id, place_name, place_type, busyness_data, success)
    """
    extractor = LiveBusynessExtractor(username, password)
    # Load proxies - need to get the proxy file
    try:
        proxy_file = get_latest_proxy_file()
        extractor.load_proxies(proxy_file)
    except Exception as e:
        logger.error(f"Failed to load proxies in worker: {e}")
        # Fallback: use only the assigned proxy
        extractor.proxies = [proxy]
    
    results = []
    
    for place_id, place_type in place_id_type_pairs:
        t0 = time.perf_counter()
        success = False
        blocked = False
        busyness_data = None
        place_name = None
        error_msg = None
        
        timezone_name = None
        try:
            # Use the assigned proxy directly
            json_url = extractor.get_json_url(place_id, proxy)
            
            if not json_url:
                blocked = True
                error_msg = "json_url_not_found"
            else:
                json_data = extractor.get_json_data(json_url, proxy, place_id)
                
                if not json_data:
                    blocked = True
                    error_msg = "json_data_not_found"
                else:
                    # Extract all fields including opening_hours and coordinates
                    extracted_busyness = extractor.extract_busyness_data(json_data)
                    place_name = extractor.extract_place_name(json_data)
                    place_address = extractor.extract_place_address(json_data)
                    timezone_name = extractor.extract_timezone(json_data) if hasattr(extractor, 'extract_timezone') else None
                    opening_hours = extractor.extract_opening_hours(json_data) if hasattr(extractor, 'extract_opening_hours') else None
                    location = extractor.extract_coordinates(json_data) if hasattr(extractor, 'extract_coordinates') else {'lat': None, 'lng': None}
                    
                    # Format opening hours as readable string
                    opening_hours_str = None
                    if opening_hours:
                        if isinstance(opening_hours, dict):
                            days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                            formatted_hours = []
                            for day in days_order:
                                if day in opening_hours:
                                    hours = opening_hours[day]
                                    if isinstance(hours, list) and hours:
                                        day_hours = '; '.join(hours)
                                        formatted_hours.append(f"{day}: {day_hours}")
                                    elif isinstance(hours, str):
                                        formatted_hours.append(f"{day}: {hours}")
                            opening_hours_str = '; '.join(formatted_hours) if formatted_hours else None
                        elif isinstance(opening_hours, str):
                            opening_hours_str = opening_hours
                    
                    # Extract coordinates
                    latitude, longitude = None, None
                    if isinstance(location, dict):
                        latitude = location.get('lat')
                        longitude = location.get('lng')
                    elif isinstance(location, (list, tuple)) and len(location) == 2:
                        latitude, longitude = location[0], location[1]
                    
                    if extracted_busyness and extracted_busyness.get("current_busyness") is not None:
                        # Store all extracted data in busyness_data dict
                        busyness_data = extracted_busyness
                        busyness_data['opening_hours'] = opening_hours_str
                        busyness_data['latitude'] = latitude
                        busyness_data['longitude'] = longitude
                        busyness_data['place_address'] = place_address
                        if timezone_name:
                            busyness_data['timezone'] = timezone_name
                        success = True
                    else:
                        blocked = True
                        error_msg = "no_busyness_data"
                    
        except Exception as e:
            error_msg = str(e)
            blocked = is_blocked_exception(e)
            logger.error(f"Error processing place_id {place_id} with proxy {proxy}: {error_msg}")
        
        response_time = (time.perf_counter() - t0) * 1000
        
        # Create result tuple
        if success:
            results.append((place_id, place_name, place_type, busyness_data, True))
        else:
            results.append((
                place_id,
                None,
                place_type,
                {
                    "current_hour": None,
                    "original_busyness": None,
                    "current_busyness": None,
                    "timestamp": datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    "place_address": None,
                    "timezone": timezone_name
                },
                False
            ))
        
        # Update progress
        with progress_lock:
            progress_counter["count"] += 1
            current = progress_counter["count"]
        
        # Print progress
        tick = "✅" if success else "❌"
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        print(f"{timestamp} - INFO - Processed {place_id}: {tick} {current}/{total_place_ids}", 
              end="\r" if current != total_place_ids else "\n", flush=True)
        
        # Apply delays or cooldowns based on success/failure
        if blocked and not success:
            cooldown = random.uniform(*cooldown_range)
            time.sleep(cooldown)
        else:
            delay = random.uniform(*delay_range)
            time.sleep(delay)
    
    return results

# Initialize Redis client
redis_client = redis.Redis(host=Config.REDIS_HOST, port=Config.REDIS_PORT, decode_responses=True)

# In-memory job storage
JOBS = {}

# Constants
import os as _os
_BASE_DIR = _os.path.dirname(_os.path.dirname(__file__))  # api_json_extraction
UPLOAD_DIR = _os.path.join(_BASE_DIR, "uploads")
_os.makedirs(UPLOAD_DIR, exist_ok=True)

async def get_username_from_api_key(api_key: str) -> str:
    """Get username from API key."""
    try:
        query = users.select().where(users.c.api_key == api_key)
        user = await database.fetch_one(query)
        if not user:
            return None
        return user["username"]
    except Exception as e:
        logger.error(f"Error getting username from API key: {e}")
        return None

async def create_job(api_key: str, file_path: str, extraction_options: dict = None) -> dict:
    """
    Create a new job for data extraction.
    
    Args:
        api_key (str): The API key of the user creating the job
        file_path (str): Path to the CSV file containing place IDs
        extraction_options (dict): Options for the extraction job, including:
            - live_busyness: Whether this is a live busyness job
            - start_date: Start date for live busyness data collection
            - end_date: End date for live busyness data collection
            - interval_minutes: Interval between data collection (default: 60)
    
    Returns:
        dict: Job metadata including job_id and status
    """
    try:
        # Get username from API key
        username = await get_username_from_api_key(api_key)
        if not username:
            logger.error(f"Invalid API key: {api_key}")
            return None
        
        # Generate job ID
        job_id = shortuuid.uuid()
        
        # Read place IDs from CSV
        df = pd.read_csv(file_path)
        total_place_ids = len(df)
        
        # Create job record
        job_data = {
            "id": job_id,
            "username": username,
            "status": "PENDING",
            "timestamp": datetime.now(),
            "last_updated": datetime.now(),
            "file_path": file_path,
            "output_file": None,
            "total_place_ids": total_place_ids,
            "processed_place_ids": 0,
            "extraction_options": extraction_options or {},
            "error_message": None
        }
        
        # Insert into database
        query = jobs.insert().values(**job_data)
        await database.execute(query)
        
        # Store in memory
        memory_job_data = job_data.copy()
        memory_job_data["timestamp"] = job_data["timestamp"].strftime("%Y-%m-%dT%H:%M:%S")
        memory_job_data["last_updated"] = job_data["last_updated"].strftime("%Y-%m-%dT%H:%M:%S")
        memory_job_data["api_key"] = api_key
        memory_job_data["canceled"] = False
        memory_job_data["pending_place_ids"] = df["place_id"].tolist()
        memory_job_data["processed_place_ids"] = []
        JOBS[job_id] = memory_job_data
        
        # If this is a live busyness job, set up the cron job
        if extraction_options and extraction_options.get("live_busyness"):
            if not extraction_options.get("start_date") or not extraction_options.get("end_date"):
                logger.error(f"Missing start_date or end_date for live busyness job {job_id}")
                return None
                
            schedule_config = {
                "start_date": extraction_options["start_date"],
                "end_date": extraction_options["end_date"],
                "interval_minutes": extraction_options.get("interval_minutes", 60)
            }
            
            if not setup_cron_job(job_id, schedule_config):
                logger.error(f"Failed to set up cron job for job {job_id}")
                # Update job status to failed
                update_query = jobs.update().where(jobs.c.id == job_id).values(
                    status="FAILED",
                    last_updated=datetime.now(),
                    error_message="Failed to set up cron job for live busyness data"
                )
                await database.execute(update_query)
                return None
        
        return {
            "job_id": job_id,
            "status": "PENDING",
            "total_place_ids": total_place_ids
        }
        
    except Exception as e:
        logger.error(f"Error creating job: {str(e)}")
        return None

async def get_job_status(job_id: str):
    """Get the current status of a job from database."""
    try:
        # Get job from database
        query = jobs.select().where(jobs.c.id == job_id)
        job = await database.fetch_one(query)
        if not job:
            logger.error(f"Job {job_id} not found in database")
            return None
        
        # Convert to dict and ensure all required fields are present
        job_dict = dict(job)
        
        # Add in-memory data if available
        if job_id in JOBS:
            memory_data = JOBS[job_id]
            # Add memory-only fields
            job_dict.update({
                "api_key": memory_data.get("api_key"),
                "canceled": memory_data.get("canceled", False),
                "pending_place_ids": memory_data.get("pending_place_ids", []),
                "processed_place_ids": memory_data.get("processed_place_ids", [])
            })
        
        # Ensure all required fields are present with default values
        required_fields = {
            "status": "PENDING",
            "total_place_ids": 0,
            "processed_place_ids": 0,
            "timestamp": datetime.now(),
            "last_updated": datetime.now(),
            "file_path": None,
            "output_file": None,
            "error_message": None,
            "extraction_options": {},
            "canceled": False,
            "pending_place_ids": [],
            "processed_place_ids": []
        }
        
        for field, default_value in required_fields.items():
            if field not in job_dict or job_dict[field] is None:
                job_dict[field] = default_value
        
        # Ensure username is present
        if "username" not in job_dict:
            logger.error(f"Job {job_id} missing username field")
            return None
            
        return job_dict
        
    except Exception as e:
        logger.error(f"Error getting job status for {job_id}: {str(e)}")
        return None

async def list_all_jobs():
    """List all jobs from database."""
    query = jobs.select()
    db_jobs = await database.fetch_all(query)
    
    # Convert to dict and merge with in-memory data
    all_jobs = {}
    for job in db_jobs:
        job_dict = dict(job)
        job_id = job_dict["id"]
        if job_id in JOBS:
            job_dict.update(JOBS[job_id])
        all_jobs[job_id] = job_dict
    
    return all_jobs

async def cancel_job(job_id: str, api_key: str = None) -> Tuple[bool, str]:
    """
    Cancel a job and clean up associated resources.
    """
    try:
        # Get job from database
        query = jobs.select().where(jobs.c.id == job_id)
        job = await database.fetch_one(query)
        if not job:
            logger.error(f"Job {job_id} not found in database")
            return False, "Job not found"
        # Check ownership if api_key is provided
        if api_key:
            query = users.select().where(users.c.api_key == api_key)
            user = await database.fetch_one(query)
            if not user:
                logger.error(f"Invalid API key provided for job cancellation: {api_key}")
                return False, "Invalid API key"
            if user["username"] != job["username"]:
                logger.error(f"Unauthorized cancellation attempt for job {job_id} by user {user['username']}")
                return False, "Not authorized to cancel this job"
        # Check if job is already in a terminal state
        if job["status"] in ["COMPLETED", "FAILED", "CANCELED"]:
            logger.info(f"Job {job_id} is already in {job['status']} state")
            return False, f"Job is already {job['status'].lower()}"
        # Update job status in database
        try:
            update_query = jobs.update().where(jobs.c.id == job_id).values(
                status="CANCELED",
                last_updated=datetime.now(),
                error_message="Job canceled by user"
            )
            await database.execute(update_query)
            logger.info(f"Updated job {job_id} status to CANCELED in database")
        except Exception as db_error:
            logger.error(f"Database error while canceling job {job_id}: {str(db_error)}")
            return False, f"Database error while canceling job: {str(db_error)}"
        # Update in-memory job data
        if job_id in JOBS:
            JOBS[job_id]["status"] = "CANCELED"
            JOBS[job_id]["canceled"] = True
            JOBS[job_id]["last_updated"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            logger.info(f"Updated in-memory data for job {job_id}")
        # If this is a live busyness job, remove the cron job
        cron_removed = False
        extraction_options = job["extraction_options"] if "extraction_options" in job and job["extraction_options"] else {}
        if extraction_options.get("live_busyness"):
            logger.info(f"Attempting to remove cron job for live busyness job {job_id}")
            cron_removed = remove_cron_job(job_id)
            if not cron_removed:
                logger.warning(f"Failed to remove cron job for job {job_id}, will escalate to manual cleanup if needed")
            else:
                logger.info(f"Successfully removed cron job for job {job_id}")
        # Clean up job files
        files_cleaned = False
        logger.info(f"Attempting to clean up files for job {job_id}")
        files_cleaned = cleanup_job_files(job_id)
        if not files_cleaned:
            logger.warning(f"Failed to clean up some files for job {job_id}, will escalate to manual cleanup if needed")
        else:
            logger.info(f"Successfully cleaned up all files for job {job_id}")
        # Release any proxies that might be in use for this job
        proxy_released = False
        try:
            proxy_key = f"job:{job_id}:proxy"
            proxy = redis_client.get(proxy_key)
            if proxy:
                release_proxy(proxy)
                redis_client.delete(proxy_key)
                proxy_released = True
                logger.info(f"Released proxy for job {job_id}")
            else:
                logger.info(f"No proxy found for job {job_id}")
        except Exception as proxy_error:
            logger.warning(f"Error releasing proxy for job {job_id}: {str(proxy_error)}")
        # If either cron or file cleanup failed, escalate to manual cleanup
        if (extraction_options.get("live_busyness") and not cron_removed) or not files_cleaned:
            logger.warning(f"Escalating to manual cleanup for job {job_id}")
            try:
                success, msg = await manual_cleanup_job(job_id)
                if success:
                    logger.info(f"Manual cleanup succeeded for job {job_id}")
                    return True, f"Job canceled and cleaned up (manual cleanup used): {msg}"
                else:
                    logger.error(f"Manual cleanup failed for job {job_id}: {msg}")
                    return False, f"Job canceled but manual cleanup failed: {msg}"
            except Exception as e:
                logger.error(f"Exception during manual cleanup for job {job_id}: {str(e)}")
                return False, f"Job canceled but manual cleanup failed: {str(e)}"
        # Prepare success message with details
        cleanup_details = []
        if cron_removed:
            cleanup_details.append("cron job removed")
        if files_cleaned:
            cleanup_details.append("files cleaned")
        if proxy_released:
            cleanup_details.append("proxy released")
        success_message = "Job canceled successfully"
        if cleanup_details:
            success_message += f" ({', '.join(cleanup_details)})"
        logger.info(f"Successfully canceled job {job_id}: {success_message}")
        return True, success_message
    except Exception as e:
        logger.error(f"Error canceling job {job_id}: {str(e)}\n{traceback.format_exc()}")
        return False, f"Error canceling job: {str(e)}"

async def process_job(job_id):
    """Process a job by extracting JSON data for each place ID."""
    try:
        logger.info(f"Starting process_job for job_id: {job_id}")
        
        # Get job from database
        query = jobs.select().where(jobs.c.id == job_id)
        job = await database.fetch_one(query)
        if not job:
            logger.error(f"Job {job_id} not found in database")
            return {"error": "Job not found"}
        
        # Update status to processing
        update_query = jobs.update().where(jobs.c.id == job_id).values(
            status="PROCESSING",
            last_updated=datetime.now()
        )
        await database.execute(update_query)
        
        # Update in-memory status
        if job_id in JOBS:
            JOBS[job_id]["status"] = "PROCESSING"
            JOBS[job_id]["last_updated"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        
        # Get in-memory job data
        job_data = JOBS.get(job_id, {})
        if not job_data:
            job_data = dict(job)
            JOBS[job_id] = job_data
        
        file_path = job_data["file_path"]
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Input file not found: {file_path}")
            
        # Create output directory inside this repository
        base_dir = str(_PROJECT_ROOT)
        output_dir = os.path.join(base_dir, "api_json_extraction", "uploads", f"job_{job_id}_results")
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Created output directory: {output_dir}")
        
        # Create or append to CSV file for extraction results
        results_csv_path = os.path.join(output_dir, "extraction_results.csv")
        logger.info(f"Will create/append to results CSV at: {results_csv_path}")
        
        # Write header only if file doesn't exist
        if not os.path.exists(results_csv_path):
            with open(results_csv_path, "w", encoding="utf-8", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([
                    "place_id", "place_name", "place_type", "current_hour",
                    "original_busyness", "current_busyness", "timestamp",
                    "opening_hours", "latitude", "longitude", "place_address",
                    "timezone"
                ])
        
        # Read place IDs from the CSV file
        df = pd.read_csv(file_path)
        place_ids = df["place_id"].tolist()
        place_types = df["place_type"].tolist() if "place_type" in df.columns else [None] * len(place_ids)
        logger.info(f"Found {len(place_ids)} place IDs to process")
        
        # Check for cancellation
        if job_data.get("canceled"):
            job_data["status"] = "CANCELED"
            update_query = jobs.update().where(jobs.c.id == job_id).values(
                status="CANCELED",
                last_updated=datetime.now()
            )
            await database.execute(update_query)
            return job_data
        
        try:
            # Initialize LiveBusynessExtractor
            extractor = LiveBusynessExtractor(
                proxy_username=DEFAULT_PROXY_USERNAME,
                proxy_password=DEFAULT_PROXY_PASSWORD
            )
            
            # Get the latest proxy file
            proxy_file = get_latest_proxy_file()
            extractor.load_proxies(proxy_file)
            
            # Check if we have proxies available
            if not extractor.proxies:
                logger.error("No proxies loaded; cannot process place IDs")
                raise Exception("No proxies available")
            
            logger.info(f"Loaded {len(extractor.proxies)} proxies for parallel processing")
            
            # Assign place IDs to proxies in round-robin fashion
            assignment = assign_place_ids_to_proxies(place_ids, place_types, extractor.proxies)
            
            # Shared progress counter and lock for global progress
            progress_counter = {"count": 0}
            progress_lock = threading.Lock()
            total_place_ids = len(place_ids)
            
            # Process place IDs in parallel using ThreadPoolExecutor
            max_workers = len(extractor.proxies)
            all_results = []
            
            logger.info(f"Starting parallel processing with {max_workers} workers for {total_place_ids} place IDs")
            
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                futures = []
                for proxy in extractor.proxies:
                    place_id_type_pairs = assignment[proxy]
                    if not place_id_type_pairs:
                        continue
                    
                    future = executor.submit(
                        process_place_id_worker,
                        proxy,
                        DEFAULT_PROXY_USERNAME,
                        DEFAULT_PROXY_PASSWORD,
                        place_id_type_pairs,
                        DELAY_RANGE,
                        COOLDOWN_RANGE,
                        progress_counter,
                        progress_lock,
                        total_place_ids
                    )
                    futures.append(future)
                
                # Collect results from all workers
                for future in as_completed(futures):
                    try:
                        worker_results = future.result()
                        all_results.extend(worker_results)
                    except Exception as e:
                        logger.error(f"Worker thread error: {e}")
            
            results = all_results
            logger.info(f"Successfully processed {len(results)} place IDs using parallel proxy processing")
            
            # Update progress in database
            update_query = jobs.update().where(jobs.c.id == job_id).values(
                processed_place_ids=len(results),
                last_updated=datetime.now()
            )
            await database.execute(update_query)
            
            # Update in-memory progress
            if job_id in JOBS:
                JOBS[job_id]["processed_place_ids"] = len(results)
                JOBS[job_id]["last_updated"] = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
            
            logger.info(f"Successfully processed {len(results)} place IDs")
            
        except Exception as e:
            logger.error(f"Error processing place IDs: {str(e)}")
            raise
        
        # Save results
        with open(results_csv_path, "a", encoding="utf-8", newline="") as f:
            writer = csv.writer(f)
            for result in results:
                busyness_data = result[3] if len(result) > 3 else {}
                writer.writerow([
                    result[0],  # place_id
                    result[1],  # place_name
                    result[2],  # place_type
                    busyness_data.get("current_hour"),
                    busyness_data.get("original_busyness"),
                    busyness_data.get("current_busyness"),
                    busyness_data.get("timestamp"),
                    busyness_data.get("opening_hours", ""),
                    busyness_data.get("latitude", ""),
                    busyness_data.get("longitude", ""),
                    busyness_data.get("place_address", ""),
                    busyness_data.get("timezone", "")
                ])
        
        # Create zip file with results
        zip_path = os.path.join(output_dir, "results.zip")
        with zipfile.ZipFile(zip_path, "w") as zipf:
            zipf.write(results_csv_path, "extraction_results.csv")
        
        # Update job status to COMPLETED
        update_query = jobs.update().where(jobs.c.id == job_id).values(
            status="COMPLETED",
            last_updated=datetime.now(),
            output_file=zip_path,
            processed_place_ids=len(results)
        )
        await database.execute(update_query)
        
        # Update in-memory status
        if job_id in JOBS:
            JOBS[job_id].update({
                "status": "COMPLETED",
                "last_updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "output_file": zip_path,
                "processed_place_ids": len(results),
                "pending_place_ids": []
            })
        
        # Clean up input file
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up input file: {file_path}")
        except Exception as e:
            logger.error(f"Error cleaning up input file {file_path}: {str(e)}")
        
        logger.info(f"Job {job_id} completed successfully. Results are in: {output_dir}")
        return job_data
        
    except Exception as e:
        logger.error(f"Error in process_job for {job_id}: {str(e)}")
        
        # Update job status to failed
        update_query = jobs.update().where(jobs.c.id == job_id).values(
            status="FAILED",
            last_updated=datetime.now(),
            error_message=str(e)
        )
        await database.execute(update_query)
        
        # Update in-memory status
        if job_id in JOBS:
            JOBS[job_id].update({
                "status": "FAILED",
                "last_updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
                "error_message": str(e)
            })
        
        # Clean up input file even if job failed
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
                logger.info(f"Cleaned up input file after failure: {file_path}")
        except Exception as cleanup_error:
            logger.error(f"Error cleaning up input file {file_path} after failure: {str(cleanup_error)}")
        
        return {"error": str(e)}

async def get_user_stats(api_key: str):
    """Get statistics for a specific user from database."""
    # Get user from API key
    user_query = users.select().where(users.c.api_key == api_key)
    user = await database.fetch_one(user_query)
    if not user:
        return None
    
    # Get all jobs for this user
    query = jobs.select().where(jobs.c.username == user["username"])
    user_jobs = await database.fetch_all(query)
    
    stats = {
        "total_jobs": len(user_jobs),
        "completed_jobs": 0,
        "failed_jobs": 0,
        "pending_jobs": 0,
        "total_place_ids": 0,
        "completed_place_ids": 0,
        "failed_place_ids": 0
    }
    
    for job in user_jobs:
        stats["total_place_ids"] += job["total_place_ids"] or 0
        stats["completed_place_ids"] += job["processed_place_ids"] or 0
        
        if job["status"] == "COMPLETED":
            stats["completed_jobs"] += 1
        elif job["status"] == "FAILED":
            stats["failed_jobs"] += 1
        elif job["status"] in ["PENDING", "PROCESSING"]:
            stats["pending_jobs"] += 1
    
    return stats

async def get_all_user_stats():
    """Get statistics for all users from database."""
    all_stats = {}
    
    # Get all users
    users_query = users.select()
    all_users = await database.fetch_all(users_query)
    
    for user in all_users:
        if user["api_key"]:
            stats = await get_user_stats(user["api_key"])
            if stats:
                all_stats[user["api_key"]] = stats
    
    return all_stats

def cleanup_job_files(job_id: str) -> bool:
    """
    Clean up all files and directories associated with a job.
    
    Args:
        job_id (str): The ID of the job to clean up
        
    Returns:
        bool: True if all files were cleaned up successfully, False if some files couldn't be cleaned up
    """
    success = True
    base_dir = str(_PROJECT_ROOT)
    
    # List of possible file locations to clean up
    cleanup_paths = [
        # Input files
        os.path.join(base_dir, "api_json_extraction", "uploads", f"job_{job_id}.csv"),
        # Output directories
        os.path.join(base_dir, "api_json_extraction", "uploads", f"job_{job_id}_results"),
        # Log files
        os.path.join(base_dir, "api_json_extraction", "logs", f"cron_busyness_{job_id}.log"),
        os.path.join(base_dir, "logs", f"cron_busyness_{job_id}.log"),
        # Script files
        os.path.join(base_dir, "api_json_extraction", "scripts", f"live_busyness_{job_id}.sh"),
        os.path.join(base_dir, "scripts", f"live_busyness_{job_id}.sh"),
        # Remove the entire POI searcher output directory
        os.path.join(base_dir, "poi_searcher/output"),
    ]
    
    for path in cleanup_paths:
        try:
            if os.path.exists(path):
                if os.path.isdir(path):
                    shutil.rmtree(path)
                    logger.info(f"Removed directory: {path}")
                else:
                    os.remove(path)
                    logger.info(f"Removed file: {path}")
        except Exception as e:
            logger.error(f"Error cleaning up {path}: {str(e)}")
            success = False
    
    # Clean up any Redis keys associated with this job
    try:
        redis_client.delete(f"job:{job_id}:proxy")
        redis_client.delete(f"job:{job_id}:status")
        redis_client.delete(f"job:{job_id}:progress")
        logger.info(f"Cleaned up Redis keys for job {job_id}")
    except Exception as e:
        logger.error(f"Error cleaning up Redis keys for job {job_id}: {str(e)}")
        success = False
    
    return success

def custom_json_serializer(obj):
    """Handle non-serializable types."""
    if isinstance(obj, (np.generic, np.ndarray)):
        return obj.item() if isinstance(obj, np.generic) else obj.tolist()
    elif isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
        return None  # Replace NaN/Infinity with null
    raise TypeError(f"Object of type {obj.__class__.__name__} is not JSON serializable")

def setup_cron_job(job_id: str, schedule_config: dict) -> bool:
    try:
        script_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        script_path = os.path.join(script_dir, "scripts", f"live_busyness_{job_id}.sh")
        
        project_root = str(_PROJECT_ROOT)
        with open(script_path, "w") as f:
            f.write(f"""#!/bin/bash

LOG_FILE="{project_root}/logs/cron_busyness_{job_id}.log"

echo "Starting busyness extraction for job {job_id} at $(date)" >> "$LOG_FILE"

cd "{project_root}"
echo "Changed to directory: $(pwd)" >> "$LOG_FILE"

export PYTHONPATH="{project_root}"
export PATH="{project_root}/monitor_sys/bin:$PATH"
echo "Environment variables set" >> "$LOG_FILE"

echo "Python path: $(which python3)" >> "$LOG_FILE"

"$PYTHONPATH"/monitor_sys/bin/python3 \
    "{project_root}/live_busyness_extraction/main.py" \\
    --proxy_username {DEFAULT_PROXY_USERNAME} \\
    --proxy_password {DEFAULT_PROXY_PASSWORD} \\
    --input_file "{project_root}/api_json_extraction/uploads/job_{job_id}.csv" \
    --output_file "{project_root}/api_json_extraction/uploads/job_{job_id}_results/extraction_results_{job_id}.csv" \
    --start_date "{schedule_config['start_date']} 00:00:00" \
    --end_date "{schedule_config['end_date']} 23:59:59"

if [ $? -eq 0 ]; then
    echo "Busyness extraction completed successfully at $(date)" >> "$LOG_FILE"
else
    echo "Busyness extraction failed at $(date)" >> "$LOG_FILE"
fi

echo -e "\\n# ================================================== Run Separator ==================================================\\n" >> "$LOG_FILE"
""")
        
        os.chmod(script_path, 0o755)
        
        cron = CronTab(user=True)
        
        interval_minutes = schedule_config.get('interval_minutes', 60)
        
        job = cron.new(command=script_path, comment=f"live_busyness_{job_id}")
        job.minute.every(interval_minutes)
        
        cron.write()
        
        logger.info(f"Successfully set up cron job for job {job_id} with comment live_busyness_{job_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error setting up cron job for job {job_id}: {str(e)}")
        return False

def remove_cron_job(job_id: str) -> bool:
    try:
        script_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        script_path = os.path.join(script_dir, "scripts", f"live_busyness_{job_id}.sh")
        if os.path.exists(script_path):
            os.remove(script_path)
            logger.info(f"Removed script file: {script_path}")
        else:
            logger.info(f"Script file not found for removal: {script_path}")
        
        cron = CronTab(user=True)
        jobs_to_remove = []
        removed_by_comment = cron.remove_all(comment=f"live_busyness_{job_id}")
        if removed_by_comment:
            logger.info(f"Removed {removed_by_comment} cron job(s) by comment for job {job_id}")
        for job in cron:
            if script_path in str(job.command):
                jobs_to_remove.append(job)
                logger.info(f"Found cron job to remove by script path: {job}")
        for job in jobs_to_remove:
            cron.remove(job)
        if jobs_to_remove:
            logger.info(f"Removed {len(jobs_to_remove)} cron job(s) by script path for job {job_id}")
        cron.write()
        if removed_by_comment or jobs_to_remove:
            logger.info(f"Successfully removed cron job(s) for job {job_id}")
            return True
        else:
            logger.warning(f"No cron jobs found to remove for job {job_id}")
            return False
    except Exception as e:
        logger.error(f"Error removing cron job for job {job_id}: {str(e)}")
        return False

async def manual_cleanup_job(job_id: str) -> Tuple[bool, str]:
    try:
        logger.info(f"Starting manual cleanup for job {job_id}")
        
        cron_removed = remove_cron_job(job_id)
        
        files_cleaned = cleanup_job_files(job_id)
        
        proxy_released = False
        try:
            proxy_key = f"job:{job_id}:proxy"
            proxy = redis_client.get(proxy_key)
            if proxy:
                release_proxy(proxy)
                redis_client.delete(proxy_key)
                proxy_released = True
        except Exception as e:
            logger.warning(f"Error releasing proxy: {e}")
        
        try:
            query = jobs.select().where(jobs.c.id == job_id)
            job = await database.fetch_one(query)
            if job:
                update_query = jobs.update().where(jobs.c.id == job_id).values(
                    status="CANCELED",
                    last_updated=datetime.now(),
                    error_message="Job manually cleaned up"
                )
                await database.execute(update_query)
                logger.info(f"Updated job {job_id} status to CANCELED in database")
        except Exception as e:
            logger.warning(f"Could not update database for job {job_id}: {e}")
        
        cleanup_details = []
        if cron_removed:
            cleanup_details.append("cron job removed")
        if files_cleaned:
            cleanup_details.append("files cleaned")
        if proxy_released:
            cleanup_details.append("proxy released")
        
        success_message = "Manual cleanup completed"
        if cleanup_details:
            success_message += f" ({', '.join(cleanup_details)})"
        
        logger.info(f"Manual cleanup completed for job {job_id}: {success_message}")
        return True, success_message
        
    except Exception as e:
        error_msg = f"Error during manual cleanup: {str(e)}"
        logger.error(f"Manual cleanup failed for job {job_id}: {error_msg}")
        return False, error_msg

def cleanup_input_file_for_completed_job(job_id: str) -> bool:
    try:
        base_dir = str(_PROJECT_ROOT)
        input_file_path = os.path.join(base_dir, "api_json_extraction", "uploads", f"job_{job_id}.csv")
        
        if os.path.exists(input_file_path):
            os.remove(input_file_path)
            logger.info(f"Cleaned up input file for completed job {job_id}: {input_file_path}")
            return True
        else:
            logger.info(f"Input file for job {job_id} not found: {input_file_path}")
            return True 

    except Exception as e:
        logger.error(f"Error cleaning up input file for job {job_id}: {str(e)}")
        return False


