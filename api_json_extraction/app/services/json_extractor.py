import requests
import re
import json
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from app.services.proxy_manager import get_proxy, release_proxy
from typing import List, Dict, Tuple, Any
from datetime import datetime

import os
from dotenv import load_dotenv

# Try to load .env from project root if not already loaded
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_PROJECT_ROOT = os.path.abspath(os.path.join(_SCRIPT_DIR, "../../.."))
env_path = os.path.join(_PROJECT_ROOT, ".env")
if os.path.exists(env_path):
    load_dotenv(env_path)

DEFAULT_PROXY_USERNAME = os.getenv("PROXY_USERNAME")
DEFAULT_PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")

# Configuration for parallel processing
DELAY_RANGE = (0.01, 0.2)  # Human-like delays in seconds
COOLDOWN_RANGE = (30, 60)  # Cooldown on proxy blocks in seconds

def get_result(url, proxy):
    try:
        if not proxy.startswith("http://") and not proxy.startswith("https://"):
            proxy = f"http://{proxy}"
        formatted_proxy = f"http://{DEFAULT_PROXY_USERNAME}:{DEFAULT_PROXY_PASSWORD}@{proxy.split('://')[-1]}"
        proxies = {"http": formatted_proxy, "https": formatted_proxy}
        response = requests.get(url, proxies=proxies, timeout=10)
        if response.status_code == 200:
            return response.content.decode('utf-8')
        else:
            print(f"Failed to fetch URL {url}. Status code: {response.status_code}")
            return None
    except Exception as e:
        print(f"Error fetching URL {url}: {e}")
        return None

def get_id_pair(google_search_content):
    pattern = r'(?!0x0:0x)0x[0-9a-f]+:0x[0-9a-f]+'
    found_text = re.findall(pattern, google_search_content)
    return found_text[0] if found_text else None

def get_pb(ids):
    """Generate the pb parameter for the JSON URL."""
    pb_template = '!1m17!1s{0}!3m12!1m3!1d442.3895626117454!2d51.52449680499801!3d25.37702882842613!2m3!1f0!2f0!3f0!3m2!1i1280!2i377!4f13.1!4m2!3d25.37705656392284!4d51.52511112391948!12m4!2m3!1i360!2i120!4i8!13m57!2m2!1i203!2i100!3m2!2i4!5b1!6m6!1m2!1i86!2i86!1m2!1i408!2i240!7m42!1m3!1e1!2b0!3e3!1m3!1e2!2b1!3e2!1m3!1e2!2b0!3e3!1m3!1e3!2b0!3e3!1m3!1e8!2b0!3e3!1m3!1e3!2b1!3e2!1m3!1e9!2b1!3e2!1m3!1e10!2b0!3e3!1m3!1e10!2b1!3e2!1m3!1e10!2b0!3e4!2b1!4b1!9b0!14m5!1sLMySXoeYBI6Ck74P-9a24AU!4m1!2i5210!7e81!12e3!15m49!1m13!4e2!13m6!2b1!3b1!4b1!6i1!8b1!9b1!18m4!3b1!4b1!5b1!6b1!2b1!5m5!2b1!3b1!5b1!6b1!7b1!10m1!8e3!14m1!3b1!17b1!20m2!1e3!1e6!24b1!25b1!26b1!30m1!2b1!36b1!43b1!52b1!54m1!1b1!55b1!56m2!1b1!3b1!65m5!3m4!1m3!1m2!1i224!2i298!21m28!1m6!1m2!1i0!2i0!2m2!1i458!2i377!1m6!1m2!1i1230!2i0!2m2!1i1280!2i377!1m6!1m2!1i0!2i0!2m2!1i1280!2i20!1m6!1m2!1i0!2i357!2m2!1i1280!2i377!22m1!1e81!29m0!30m1!3b1'
    return pb_template.format(ids)

def get_json_url(pb):
    json_url_template = "https://www.google.com/maps/preview/place?authuser=0&hl=en&gl=de&pb={0}&pf=t"
    return json_url_template.format(pb)

def process_place_id(place_id, proxy):
    """Process a single place_id to generate its JSON URL."""
    google_search_url = f"https://www.google.com/maps/place/?q=place_id:{place_id}"
    google_content = get_result(google_search_url, proxy)
    if not google_content:
        print(f"Failed to fetch content for place_id {place_id}")
        return None
    id_pair = get_id_pair(google_content)
    if not id_pair:
        print(f"No id_pair found for place_id {place_id}")
        return None
    pb = get_pb(id_pair)
    json_url = get_json_url(pb)
    
    return json_url

def assign_place_ids_to_proxies(place_ids_and_json_urls: List[Tuple[str, str]], proxies: List[str]) -> Dict[str, List[Tuple[str, str]]]:
    """Assign place_ids and json_urls to proxies in round-robin fashion."""
    assignment = {proxy: [] for proxy in proxies}
    for idx, (place_id, json_url) in enumerate(place_ids_and_json_urls):
        proxy = proxies[idx % len(proxies)]
        assignment[proxy].append((place_id, json_url))
    return assignment

def is_blocked_exception(e: Exception) -> bool:
    """Heuristic to determine if an exception indicates proxy blocking."""
    return True

def fetch_json_worker(
    proxy: str,
    place_id_url_pairs: List[Tuple[str, str]],
    delay_range: Tuple[float, float] = DELAY_RANGE,
    cooldown_range: Tuple[float, float] = COOLDOWN_RANGE,
    progress_counter: Dict[str, int] = None,
    progress_lock: threading.Lock = None,
    total_items: int = None
) -> List[Tuple[str, Dict[str, Any]]]:
    """
    Worker function to fetch JSON data for place IDs assigned to a specific proxy.
    Uses the assigned proxy exclusively with delays and cooldowns.
    
    Returns:
        List of tuples: (place_id, json_data) for successful fetches
    """
    results = []
    
    for place_id, json_url in place_id_url_pairs:
        t0 = time.perf_counter()
        success = False
        blocked = False
        json_data = None
        error_msg = None
        
        try:
            formatted_proxy = f"http://{DEFAULT_PROXY_USERNAME}:{DEFAULT_PROXY_PASSWORD}@{proxy.split('://')[-1]}"
            proxies = {"http": formatted_proxy, "https": formatted_proxy}
            response = requests.get(json_url, proxies=proxies, timeout=10)
            
            if response.status_code == 200:
                content = response.content.decode('utf-8')
                # Handle potential XSSI prefix
                if content.startswith(")]}'"):
                    content = content[4:]
                json_data = json.loads(content)
                success = True
            else:
                blocked = True
                error_msg = f"status_{response.status_code}"
                
        except Exception as e:
            error_msg = str(e)
            blocked = is_blocked_exception(e)
        
        response_time = (time.perf_counter() - t0) * 1000
        
        if success and json_data:
            results.append((place_id, json_data))
        
        # Update progress if tracking is enabled
        if progress_counter is not None and progress_lock is not None and total_items is not None:
            with progress_lock:
                progress_counter["count"] += 1
                current = progress_counter["count"]
            
            tick = "✅" if success else "❌"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{timestamp} - INFO - Processed {place_id}: {tick} {current}/{total_items}", 
                  end="\r" if current != total_items else "\n", flush=True)
        
        # Apply delays or cooldowns
        if blocked and not success:
            cooldown = random.uniform(*cooldown_range)
            time.sleep(cooldown)
        else:
            delay = random.uniform(*delay_range)
            time.sleep(delay)
    
    return results

def get_json_results(place_ids_and_json_urls, trial=1, proxies=None, use_multiprocessing=False):
    """
    Get JSON results for place IDs and their JSON URLs.
    
    Args:
        place_ids_and_json_urls: List of (place_id, json_url) tuples
        trial: Retry attempt number (default: 1)
        proxies: List of proxy strings to use (optional, uses Redis if not provided)
        use_multiprocessing: If True, uses multiprocessing with proxy assignment (default: False)
    
    Returns:
        Tuple of (json_results, failed_results)
    """
    # If multiprocessing mode and proxies provided, use worker pattern
    if use_multiprocessing and proxies and len(proxies) > 0:
        # Assign place IDs to proxies
        assignment = assign_place_ids_to_proxies(place_ids_and_json_urls, proxies)
        max_workers = len(proxies)
        
        # Shared progress tracking
        progress_counter = {"count": 0}
        progress_lock = threading.Lock()
        total_items = len(place_ids_and_json_urls)
        
        json_results = []
        failed_results = []
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for proxy in proxies:
                pairs = assignment[proxy]
                if not pairs:
                    continue
                
                future = executor.submit(
                    fetch_json_worker,
                    proxy,
                    pairs,
                    DELAY_RANGE,
                    COOLDOWN_RANGE,
                    progress_counter,
                    progress_lock,
                    total_items
                )
                futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                try:
                    worker_results = future.result()
                    # worker_results is a list of (place_id, json_data) tuples for successful fetches
                    json_results.extend(worker_results)
                except Exception as e:
                    print(f"Worker thread error: {e}")
        
        # Track failed results for retry
        processed_place_ids = {pid for pid, _ in json_results}
        for place_id, json_url in place_ids_and_json_urls:
            if place_id not in processed_place_ids:
                failed_results.append((place_id, json_url))
        
        if failed_results and trial < 3:
            retry_results, new_failed = get_json_results(failed_results, trial + 1, proxies, use_multiprocessing)
            json_results.extend(retry_results)
            failed_results = new_failed
        
        return json_results, failed_results
    
    # Original implementation using Redis proxy manager
    def fetch_json(place_id, json_url):
        proxy = get_proxy()
        if not proxy:
            print(f"No proxies available for {place_id}. Skipping...")
            return None
        try:
            formatted_proxy = f"http://{DEFAULT_PROXY_USERNAME}:{DEFAULT_PROXY_PASSWORD}@{proxy.split('://')[-1]}"
            proxies = {"http": formatted_proxy, "https": formatted_proxy}
            response = requests.get(json_url, proxies=proxies, timeout=10)
            if response.status_code == 200:
                content = response.content.decode('utf-8')
                # Handle potential XSSI prefix
                if content.startswith(")]}'"):
                    content = content[4:]
                return (place_id, json.loads(content))
            else:
                print(f"Failed to fetch URL {json_url}. Status code: {response.status_code}")
                return None
        except Exception as e:
            print(f"Error fetching URL {json_url}: {e}")
            return None
        finally:
            release_proxy(proxy)
    
    json_results = []
    failed_results = []
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_json, pid, url) for pid, url in place_ids_and_json_urls]
        for i, future in enumerate(futures):
            result = future.result()
            if result:
                json_results.append(result)
            else:
                failed_results.append(place_ids_and_json_urls[i])
                
    if failed_results and trial < 3:
        retry_results, new_failed = get_json_results(failed_results, trial + 1, proxies, use_multiprocessing)
        json_results.extend(retry_results)
        failed_results = new_failed
        
    return json_results, failed_results