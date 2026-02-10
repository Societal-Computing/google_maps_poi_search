import os as _os
import sys as _sys
from pathlib import Path

# Resolve important project paths relative to this file so the code works
# no matter where the repository is checked out (e.g. *_remote, different users, etc.).
_THIS_FILE = Path(__file__).resolve()
_APP_DIR = _THIS_FILE.parent                              # api_json_extraction/app
_API_ROOT = _APP_DIR.parent                               # api_json_extraction
_PROJECT_ROOT = _API_ROOT.parent                          # repository root

# Ensure both the api_json_extraction package (so `app.*` imports work)
# and project root (for sibling top-level packages) are importable.
for _p in (_API_ROOT, _PROJECT_ROOT):
    _p_str = str(_p)
    if _p_str not in _sys.path:
        _sys.path.insert(0, _p_str)

from fastapi import FastAPI, Depends, HTTPException, UploadFile, File, Request, BackgroundTasks, Path, Header, Body
from app.models import UserCreate
from pydantic import BaseModel
from app.dependencies import get_current_user, get_current_user_from_session
from app.services.proxy_manager import get_proxy, release_proxy, list_all_proxies, load_proxies_from_file
from db.models import users, jobs, hash_password, verify_password
from db.database import database, connect_db, disconnect_db, create_tables
import secrets, os
from app.services.job_manager import create_job, get_job_status, list_all_jobs, process_job, cancel_job, get_user_stats, get_all_user_stats
import pandas as pd
from fastapi.responses import FileResponse, JSONResponse
from fastapi import status, Form
from fastapi.responses import StreamingResponse
import zipfile
import io
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse, RedirectResponse
from app.services.pipeline_bridge import PipelineBridge
import secrets
import redis
from typing import List, Optional
from config.config import Config
from datetime import datetime
from redis.exceptions import RedisError
import shortuuid
import shutil
import logging
import shortuuid
from crontab import CronTab
import subprocess
import json
import glob
import sys
import tempfile
import time
import random
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import itertools
from typing import Dict, Any, List, Tuple
from contextlib import asynccontextmanager

# With _PROJECT_ROOT on sys.path (see top), these imports work regardless of checkout path
from live_busyness_extraction.extractor import LiveBusynessExtractor
from live_busyness_extraction.main import get_latest_proxy_file


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
# =========================
# Nominatim proxy utilities
# =========================
_NOM_LOCK = threading.Lock()
_NOM_PROXIES: List[str] = []
_NOM_CYCLE = None

def _init_nominatim_proxies():
    global _NOM_PROXIES, _NOM_CYCLE
    try:
        extractor = LiveBusynessExtractor(Config.PROXY_USERNAME, Config.PROXY_PASSWORD)
        proxy_file = get_latest_proxy_file()
        extractor.load_proxies(proxy_file)
        _NOM_PROXIES = list(extractor.proxies) if extractor.proxies else []
        random.shuffle(_NOM_PROXIES)
        _NOM_CYCLE = itertools.cycle(_NOM_PROXIES) if _NOM_PROXIES else None
        logger.info(f"Initialized Nominatim proxy pool with {_NOM_PROXIES and len(_NOM_PROXIES) or 0} proxies")
    except Exception as e:
        logger.error(f"Failed to initialize Nominatim proxies: {e}")
        _NOM_PROXIES = []
        _NOM_CYCLE = None

def _next_nominatim_proxy() -> str:
    global _NOM_CYCLE
    with _NOM_LOCK:
        if _NOM_CYCLE is None:
            _init_nominatim_proxies()
        try:
            return next(_NOM_CYCLE) if _NOM_CYCLE else None
        except Exception:
            _init_nominatim_proxies()
            return next(_NOM_CYCLE) if _NOM_CYCLE else None

def _format_proxy(proxy: str, username: str, password: str) -> dict:
    if not proxy:
        return None
    target = proxy.split('://')[-1]
    auth = f"{username}:{password}@"
    url = f"http://{auth}{target}"
    return {"http": url, "https": url}

def _nominatim_search_impl(q: str, limit: int = 5, addressdetails: int = 1, namedetails: int = 1, dedupe: int = 1, lang: str = "en"):
    # Basic guard: require at least 2 characters
    if not q or len(q.strip()) < 2:
        return []
    base = "https://nominatim.openstreetmap.org/search"
    params = {
        "format": "json",
        "q": q,
        "limit": max(1, min(limit, 10)),
        "addressdetails": addressdetails,
        "namedetails": namedetails,
        "dedupe": dedupe,
        "accept-language": lang or "en",
        "polygon_geojson": 1,
    }
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": params["accept-language"],
    }
    # Try a few proxies, then fall back to direct if all fail
    for attempt in range(3):
        px = _next_nominatim_proxy()
        proxies = _format_proxy(px, Config.PROXY_USERNAME, Config.PROXY_PASSWORD) if px else None
        try:
            resp = requests.get(base, params=params, headers=headers, proxies=proxies, timeout=6)
            if resp.status_code == 429 and attempt == 0:
                time.sleep(1)
                continue
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            logger.warning(f"Nominatim attempt {attempt+1} failed via proxy {px}: {e}")
            continue
    # Final attempt without proxy
    try:
        resp = requests.get(base, params=params, headers=headers, timeout=6)
        if resp.status_code == 429:
            time.sleep(1)
            resp = requests.get(base, params=params, headers=headers, timeout=6)
        resp.raise_for_status()
        return resp.json()
    except Exception as e:
        logger.error(f"Nominatim direct attempt failed: {e}")
        return []

# Simple session storage
SESSIONS = {}

# Helper function to get current user from session
async def get_current_user_from_session(request: Request):
    session_id = request.cookies.get("session_id")
    if not session_id or session_id not in SESSIONS:
        return None
    
    return SESSIONS[session_id]

# Helper function to get user by API key
async def get_user_by_api_key(api_key: str):
    """Get user by API key from database."""
    try:
        query = users.select().where(users.c.api_key == api_key)
        user = await database.fetch_one(query)
        return user
    except Exception as e:
        print(f"Error getting user by API key: {str(e)}")
        return None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler replacing deprecated startup/shutdown events.
    Sets up DB, proxies and Redis on startup, and disconnects DB on shutdown.
    """
    try:
        await connect_db()
        create_tables()

        # Load proxies and verify
        try:
            num_proxies = load_proxies_from_file(Config.PROXY_FILE)
            if num_proxies == 0:
                print("Warning: No proxies were loaded. Check if proxies.txt exists and has content.")
            else:
                print(f"Successfully loaded {num_proxies} proxies")
        except Exception as e:
            print(f"Error loading proxies during startup: {e}")

        # Verify Redis connection
        try:
            redis_client.ping()
        except redis.ConnectionError as e:
            print(f"Redis connection failed: {e}")
            raise
        else:
            print("Redis connection successful")

        # Hand over control to the application
        yield
    except Exception as e:
        print(f"Error during startup: {e}")
        raise
    finally:
        try:
            await disconnect_db()
        except Exception as e:
            print(f"Error during shutdown: {e}")


app = FastAPI(lifespan=lifespan)

# Paths for templates and static assets relative to the repo root
_WEB_ROOT = _PROJECT_ROOT / "web_app"
templates = Jinja2Templates(directory=str(_WEB_ROOT / "templates"))
app.mount("/static", StaticFiles(directory=str(_WEB_ROOT / "static")), name="static")

# Initialize the pipeline bridge, pointing it at this repo's poi_searcher
pipeline_bridge = PipelineBridge(poi_searcher_path=str(_PROJECT_ROOT / "poi_searcher"))

# Ensure uploads directory is always inside api_json_extraction
_APP_BASE_DIR = str(_API_ROOT)  # api_json_extraction directory
UPLOAD_DIR = os.path.join(_APP_BASE_DIR, "uploads")
os.makedirs(UPLOAD_DIR, exist_ok=True)

redis_client = redis.Redis(host=Config.REDIS_HOST, port=Config.REDIS_PORT, decode_responses=True)

class ProxyRelease(BaseModel):
    proxy: str 

class JobRequest(BaseModel):
    place_ids: List[str]
    place_types: List[str]

JOBS = {}

@app.get("/nominatim/search")
async def nominatim_search(q: str, limit: int = 5, addressdetails: int = 1, namedetails: int = 1, dedupe: int = 1, lang: str = "en"):
    return _nominatim_search_impl(q=q, limit=limit, addressdetails=addressdetails, namedetails=namedetails, dedupe=dedupe, lang=lang)

@app.post("/register")
async def register(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
    background_tasks: BackgroundTasks = None
):
    try:
        # Check for duplicate username
        query = users.select().where(users.c.username == username)
        existing_user = await database.fetch_one(query)
        if existing_user:
            return templates.TemplateResponse(
                "register.html", 
                {"request": request, "error": "Username already taken"}
            )
        
        # Create new user
        hashed_password = hash_password(password)
        api_key = secrets.token_urlsafe(32)
        query = users.insert().values(
            username=username,
            hashed_password=hashed_password,
            api_key=api_key
        )
        await database.execute(query)
        
        # Redirect to login
        response = RedirectResponse(url="/login", status_code=303)
        return response
    except Exception as e:
        print(f"Error registering user: {e}")
        return templates.TemplateResponse(
            "register.html", 
            {"request": request, "error": "Registration failed. Please try again."}
        )


@app.get("/register", response_class=HTMLResponse)
async def register_page(request: Request):
    return templates.TemplateResponse("register.html", {"request": request})


@app.get("/protected")
async def protected_route(current_user=Depends(get_current_user)):
    return {"message": f"Hello, {current_user['username']}! Your API key is valid."}

@app.get("/proxies/get")
async def get_proxy_endpoint(current_user=Depends(get_current_user)):
    proxy = get_proxy()
    if not proxy:
        raise HTTPException(status_code=404, detail="No proxies available")
    return {"proxy": proxy}

@app.post("/proxies/release")
async def release_proxy_endpoint(proxy_data: ProxyRelease, current_user=Depends(get_current_user)):
    success = release_proxy(proxy_data.proxy)
    if not success:
        raise HTTPException(status_code=400, detail="Invalid proxy or proxy not in use.")
    return {"message": "Proxy released successfully"}

@app.get("/proxies/list")
async def list_proxies_endpoint(current_user=Depends(get_current_user)):
    proxies = list_all_proxies()
    return {"proxies": proxies}

# Ensure this exists
async def get_admin_user(request: Request):
    session = await get_current_user_from_session(request)
    if not session or not session.get("is_admin") or session.get("username") != "i2sc":
        return templates.TemplateResponse(
            "admin_login.html",
            {"request": request, "error": "Admin access required. Please log in as admin."}
        )
    return session


@app.post("/submit_job")
async def submit_job(
    request: Request,
    file: UploadFile = File(...),
    # Extraction options with default values
    live_busyness: bool = Form(False),
    current_user=Depends(get_current_user),
    background_tasks: BackgroundTasks = None,
):
    try:
        if not file.filename.endswith(".csv"):
            raise HTTPException(status_code=400, detail="Uploaded file must be a CSV.")
        file_path = os.path.join(UPLOAD_DIR, file.filename)
        with open(file_path, "wb") as f:
            f.write(await file.read())
        df = pd.read_csv(file_path)
        if "place_id" not in df.columns:
            raise HTTPException(status_code=400, detail="CSV file must contain 'place_id' column.")
        
        # Create extraction options dictionary
        extraction_options = {
            "live_busyness": live_busyness
        }
        
        # Extract or generate job_id
        job_id = None
        if file.filename.startswith("job_") and file.filename.endswith(".csv"):
            job_id = file.filename[4:-4]
        if not job_id:
            job_id = shortuuid.uuid()

        # Append to the single shared cumulative CSV under live_busyness_extraction/input
        # Normalize required columns and de-duplicate by place_id+city
        # Consolidate location/city into a single normalized 'city' column (first 3 comma-separated parts)
        if "city" in df.columns:
            df["city"] = df["city"].astype(str)
        elif "location" in df.columns:
            df["city"] = df["location"].astype(str)
        else:
            df["city"] = ""
        def _normalize_city(val: str) -> str:
            try:
                parts = [p.strip() for p in str(val).split(',') if p.strip()]
                return ", ".join(parts[:3])
            except Exception:
                return str(val) if val is not None else ""
        df["city"] = df["city"].map(_normalize_city)
        if "location" in df.columns:
            try:
                df = df.drop(columns=["location"])
            except Exception:
                pass
        if "job_id" not in df.columns:
            df["job_id"] = job_id
        if "start_date" not in df.columns:
            df["start_date"] = ""
        if "end_date" not in df.columns:
            df["end_date"] = ""
        if "place_type" not in df.columns:
            df["place_type"] = ""
        # Normalize opening hours header; ensure json_url/opening_hours columns exist (empty allowed)
        if "openingHours" in df.columns and "opening_hours" not in df.columns:
            df = df.rename(columns={"openingHours": "opening_hours"})
        if "Opening Hours" in df.columns and "opening_hours" not in df.columns:
            df = df.rename(columns={"Opening Hours": "opening_hours"})
        if "json_url" not in df.columns:
            df["json_url"] = ""
        if "opening_hours" not in df.columns:
            df["opening_hours"] = ""

        # Canonical cumulative CSV path
        cumulative_dir = "/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/live_busyness_extraction/input"
        os.makedirs(cumulative_dir, exist_ok=True)
        cumulative_csv_path = os.path.join(cumulative_dir, "poi_results_cumulative.csv")

        # De-duplicate incoming rows
        df["place_id"] = df["place_id"].astype(str)
        df["city"] = df["city"].astype(str)
        df_incoming = df.drop_duplicates(subset=["place_id", "city"], keep="first")

        # Merge de-dup against existing cumulative file if present
        if os.path.exists(cumulative_csv_path) and os.path.getsize(cumulative_csv_path) > 0:
            try:
                existing = pd.read_csv(cumulative_csv_path)
                # Drop legacy columns if present and ensure required columns exist
                drop_cols = [
                    "location", "geometry_class", "geometry_type", "address_type", "admin_level"
                ]
                for _c in drop_cols:
                    if _c in existing.columns:
                        try:
                            existing = existing.drop(columns=[_c])
                        except Exception:
                            pass
                if "json_url" not in existing.columns:
                    existing["json_url"] = ""
                if "opening_hours" not in existing.columns:
                    existing["opening_hours"] = ""
                if "place_id" in existing.columns and "city" in existing.columns:
                    existing["place_id"] = existing["place_id"].astype(str)
                    existing["city"] = existing["city"].astype(str)
                    merged = pd.concat([existing, df_incoming], ignore_index=True)
                    merged = merged.drop_duplicates(subset=["place_id", "city"], keep="first")
                    base_cols = ["place_id", "city", "job_id", "start_date", "end_date", "place_type", "json_url", "opening_hours"]
                    columns_order = base_cols + [c for c in merged.columns if c not in set(base_cols)]
                    merged.to_csv(cumulative_csv_path, index=False, columns=columns_order)
                else:
                    # Fallback write if schema differs
                    base_cols = ["place_id", "city", "job_id", "start_date", "end_date", "place_type", "json_url", "opening_hours"]
                    for c in ["json_url", "opening_hours"]:
                        if c not in df_incoming.columns:
                            df_incoming[c] = ""
                    columns_order = base_cols + [c for c in df_incoming.columns if c not in set(base_cols)]
                    df_incoming.to_csv(cumulative_csv_path, mode='a', header=False, index=False, columns=columns_order)
            except Exception:
                base_cols = ["place_id", "city", "job_id", "start_date", "end_date", "place_type", "json_url", "opening_hours"]
                for c in ["json_url", "opening_hours"]:
                    if c not in df_incoming.columns:
                        df_incoming[c] = ""
                columns_order = base_cols + [c for c in df_incoming.columns if c not in set(base_cols)]
                df_incoming.to_csv(cumulative_csv_path, mode='a', header=False, index=False, columns=columns_order)
        else:
            base_cols = ["place_id", "city", "job_id", "start_date", "end_date", "place_type", "json_url", "opening_hours"]
            for c in ["json_url", "opening_hours"]:
                if c not in df_incoming.columns:
                    df_incoming[c] = ""
            columns_order = base_cols + [c for c in df_incoming.columns if c not in set(base_cols)]
            df_incoming.to_csv(cumulative_csv_path, mode='w', header=True, index=False, columns=columns_order)

        # Create/refresh the single shared script and cron that points to the cumulative CSV
        script_dir = os.path.join("/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/scripts")
        os.makedirs(script_dir, exist_ok=True)
        shared_script = os.path.join(script_dir, "live_busyness_shared.sh")
        with open(shared_script, "w") as f:
            f.write(f"""#!/bin/bash

LOG_HELPER="/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/scripts/log_to_parquet.py"
LOCK_FILE="/tmp/live_busyness_shared.lock"

log_to_parquet() {{
    echo "$1" | python3 "$LOG_HELPER" --level "$2" 2>&1 || echo "Failed to log: $1" >&2
}}

# Check if another instance is already running
if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        log_to_parquet "Another instance is already running (PID: $PID). Skipping this run at $(date)" "WARNING"
        exit 0
    else
        # Stale lock file, remove it
        rm -f "$LOCK_FILE"
    fi
fi

# Create lock file
echo $$ > "$LOCK_FILE"
trap "rm -f '$LOCK_FILE'" EXIT

log_to_parquet "Starting shared busyness extraction at $(date) (PID: $$)" "INFO"
cd /home/hewanshrestha/Desktop/i2sc/testing_monitoring_system || {{
    log_to_parquet "ERROR: Failed to change directory" "ERROR"
    exit 1
}}
export PYTHONPATH=/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system
export PATH=/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/monitor_sys/bin:$PATH
export TZ=Europe/Berlin

CMD_PY=$(which python3)
log_to_parquet "Running shared: $CMD_PY live_busyness_extraction/main.py --input_file {cumulative_csv_path} --output_file /home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/live_busyness_extraction/output/placeholder.json" "INFO"

# Run Python script and capture output, writing to parquet in real-time
log_to_parquet "Python script started, capturing output..." "INFO"
$CMD_PY \
  /home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/live_busyness_extraction/main.py \
  --proxy_username {Config.PROXY_USERNAME} \
  --proxy_password {Config.PROXY_PASSWORD} \
  --input_file "{cumulative_csv_path}" \
  --output_file "/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/live_busyness_extraction/output/placeholder.json" 2>&1 | while IFS= read -r line; do
    log_to_parquet "$line" "INFO"
done
EXIT_CODE=${{PIPESTATUS[0]}}

log_to_parquet "Python script finished with exit code: $EXIT_CODE" "INFO"

if [ $EXIT_CODE -eq 0 ]; then
  log_to_parquet "Shared busyness extraction completed successfully at $(date)" "INFO"
else
  log_to_parquet "Shared busyness extraction failed at $(date)" "ERROR"
fi

log_to_parquet "=======================================================" "INFO"
""")
        os.chmod(shared_script, 0o755)

        # Append a static shared cronjob if not already present; do not modify/remove existing cronjobs
        try:
            result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
            current_cron = result.stdout if result.returncode == 0 else ""
            cron_line = f"0 * * * * {shared_script}"
            if cron_line not in current_cron and shared_script not in current_cron:
                new_cron = (current_cron.rstrip("\n") + "\n" + cron_line + "\n").lstrip("\n")
                p = subprocess.run(["crontab", "-"], input=new_cron, text=True, capture_output=True)
                if p.returncode != 0:
                    raise RuntimeError(p.stderr.strip() or "Failed to write crontab")
        except Exception as _cron_err:
            logger.warning(f"Skipping cron registration (non-fatal): {_cron_err}")

        # Record job in DB and schedule background processing bookkeeping
        job_metadata = create_job(
            api_key=current_user["api_key"], 
            file_path=file_path,
            extraction_options=extraction_options
        )
        background_tasks.add_task(process_job, job_metadata["job_id"])
        return {"message": "Job created successfully", "job_id": job_metadata["job_id"]}
    except Exception as e:
        print(f"Error creating job: {e}")
        raise HTTPException(status_code=500, detail="Failed to create job.")

@app.get("/admin/login", response_class=HTMLResponse)
async def admin_login_page(request: Request):
    return templates.TemplateResponse("admin_login.html", {"request": request})

@app.post("/login")
async def login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...)
):
    query = users.select().where(users.c.username == username)
    user = await database.fetch_one(query)
    if not user or not verify_password(password, user["hashed_password"]):
        return templates.TemplateResponse(
            "login.html", 
            {"request": request, "error": "Invalid username or password"}
        )
    
    session_id = secrets.token_urlsafe(32)
    SESSIONS[session_id] = {
        "username": user["username"],
        "api_key": user["api_key"],
        "is_admin": user["username"] == "i2sc" and password == "i2sc@password"  # Only set admin for specific credentials
    }
    
    response = RedirectResponse(url="/", status_code=303)
    response.set_cookie(key="session_id", value=session_id)
    return response

@app.post("/admin/login")
async def admin_login(
    request: Request,
    username: str = Form(...),
    password: str = Form(...),
):
    try:
        # Check for specific admin credentials
        if username != "i2sc" or password != "i2sc@password":
            print(f"Admin login attempt failed: Invalid credentials for {username}")
            return templates.TemplateResponse(
                "admin_login.html", 
                {"request": request, "error": "Invalid admin credentials"}
            )
        
        # Create admin session
        session_id = secrets.token_urlsafe(32)
        SESSIONS[session_id] = {
            "username": username,
            "api_key": "admin",  # Special API key for admin
            "is_admin": True
        }
        
        print(f"Admin login successful for user {username}")
        
        # Redirect to admin dashboard
        response = RedirectResponse(url="/admin", status_code=303)
        response.set_cookie(key="session_id", value=session_id)
        return response
    except Exception as e:
        print(f"Error during admin login: {str(e)}")
        import traceback
        print(f"Full error traceback: {traceback.format_exc()}")
        return templates.TemplateResponse(
            "admin_login.html", 
            {"request": request, "error": "An error occurred during login. Please try again."}
        )

# Admin Users Page
@app.get("/admin/users", response_class=HTMLResponse)
async def admin_users(request: Request, current_user=Depends(get_admin_user)):
    try:
        # Get all users from database
        users_query = users.select()
        users_list = await database.fetch_all(users_query)
        
        # Convert users to list of dictionaries for easier manipulation
        users_data = []
        for user in users_list:
            user_dict = dict(user)
            # Get job statistics for this user
            user_stats = await get_user_stats(user_dict["api_key"])
            user_dict.update(user_stats)
            users_data.append(user_dict)
        
        return templates.TemplateResponse(
            "admin_users.html", 
            {
                "request": request, 
                "current_user": current_user, 
                "users": users_data
            }
        )
    except Exception as e:
        print(f"Error in admin users page: {e}")
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "error": "An error occurred while loading the users page. Please try again."
            }
        )

# Add this function after other Redis-related functions
def clear_redis_data():
    """Clear all job-related data from Redis"""
    try:
        # Get all keys related to jobs
        job_keys = redis_client.keys("job:*")
        if job_keys:
            redis_client.delete(*job_keys)
        
        # Clear any other job-related keys
        redis_client.delete("proxies_in_use")
        print("Successfully cleared Redis data")
        return True
    except RedisError as e:
        print(f"Error clearing Redis data: {e}")
        return False

# Modify the admin_jobs function
@app.get("/admin/jobs", response_class=HTMLResponse)
async def admin_jobs(request: Request, current_user=Depends(get_admin_user)):
    try:
        # Get all jobs
        all_jobs = await list_all_jobs()
        
        # Get all users from database
        query = users.select()
        users_list = await database.fetch_all(query)
        users_dict = {user["username"]: user["username"] for user in users_list}
        
        # Add username and search details to each job
        jobs_with_details = {}
        for job_id, job in all_jobs.items():
            # Only include jobs that have a valid user
            if job["username"] in users_dict:
                job_data = job.copy()
                job_data["username"] = users_dict[job["username"]]
                
                # Format timestamp for template
                if isinstance(job_data.get("timestamp"), datetime):
                    job_data["timestamp"] = job_data["timestamp"].strftime("%Y-%m-%dT%H:%M:%S")
                
                # Initialize extraction_options if it doesn't exist
                if "extraction_options" not in job_data:
                    job_data["extraction_options"] = {}
                
                # Extract search details from file path if file exists and extraction_options are empty
                file_path = job.get("file_path", "")
                if file_path and os.path.exists(file_path) and not job_data["extraction_options"].get("city"):
                    try:
                        # Read the first few lines of the CSV to get search details
                        df = pd.read_csv(file_path, nrows=1)
                        
                        # Add city and place_type from the CSV file only if not already in extraction_options
                        if "city" in df.columns and not job_data["extraction_options"].get("city"):
                            job_data["extraction_options"]["city"] = df["city"].iloc[0]
                        if "place_type" in df.columns and not job_data["extraction_options"].get("place_type"):
                            job_data["extraction_options"]["place_type"] = df["place_type"].iloc[0]
                            
                    except Exception as e:
                        print(f"Error reading job file {file_path}: {e}")
                        # Continue without search details
                
                jobs_with_details[job_id] = job_data
        
        return templates.TemplateResponse(
            "admin_jobs.html", 
            {
                "request": request, 
                "current_user": current_user, 
                "jobs": jobs_with_details
            }
        )
    except Exception as e:
        print(f"Error in admin jobs page: {e}")
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "error": "An error occurred while loading the jobs page. Please try again."
            }
        )

# Admin Proxies Page
@app.get("/admin/proxies", response_class=HTMLResponse)
async def admin_proxies(request: Request, current_user=Depends(get_admin_user)):
    try:
        all_proxies = list_all_proxies()
        proxies_in_use = redis_client.smembers("proxies_in_use")
        return templates.TemplateResponse(
            "admin_proxies.html", 
            {
                "request": request, 
                "current_user": current_user,
                "all_proxies": all_proxies,
                "in_use": proxies_in_use
            }
        )
    except Exception as e:
        print(f"Error in admin proxies page: {e}")
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "error": "An error occurred while loading the proxies page. Please try again."
            }
        )

# Admin User Statistics Page
@app.get("/admin/user_stats", response_class=HTMLResponse)
async def admin_user_stats(request: Request, current_user=Depends(get_admin_user)):
    # Get all users from database
    users_query = users.select()
    users_list = await database.fetch_all(users_query)
    
    # Get job statistics for each user
    user_stats = get_all_user_stats()
    
    # Combine user data with their statistics
    users_with_stats = []
    for user in users_list:
        stats = user_stats.get(user["api_key"], {})
        users_with_stats.append({
            "username": user["username"],
            "api_key": user["api_key"],
            "is_admin": user["is_admin"],
            "total_jobs": stats.get("total_jobs", 0),
            "completed_jobs": stats.get("completed_jobs", 0),
            "failed_jobs": stats.get("failed_jobs", 0),
            "running_jobs": stats.get("running_jobs", 0),
            "total_place_ids_processed": stats.get("total_place_ids_processed", 0),
            "last_job_timestamp": stats.get("last_job_timestamp")
        })
    
    return templates.TemplateResponse(
        "admin_user_stats.html",
        {
            "request": request,
            "current_user": current_user,
            "users": users_with_stats
        }
    )

# Admin Dashboard with Summary
@app.get("/admin", response_class=HTMLResponse)
async def admin_dashboard(request: Request, current_user=Depends(get_admin_user)):
    try:
        # Get overall statistics
        all_stats = await get_all_user_stats()
        total_users = len(all_stats)
        total_jobs = sum(stats["total_jobs"] for stats in all_stats.values())
        pending_jobs = sum(stats["pending_jobs"] for stats in all_stats.values())
        total_place_ids = sum(stats.get("total_place_ids", 0) for stats in all_stats.values())
        
        # Get proxy statistics
        all_proxies = list_all_proxies()
        proxies_in_use = redis_client.smembers("proxies_in_use")
        proxy_stats = {
            "total": len(all_proxies),
            "in_use": len(proxies_in_use),
            "available": len(all_proxies) - len(proxies_in_use)
        }
        
        return templates.TemplateResponse(
            "admin_dashboard.html",
            {
                "request": request,
                "current_user": current_user,
                "total_users": total_users,
                "total_jobs": total_jobs,
                "pending_jobs": pending_jobs,
                "total_place_ids": total_place_ids,
                "proxy_stats": proxy_stats
            }
        )
    except Exception as e:
        print(f"Error in admin dashboard: {str(e)}")
        return templates.TemplateResponse(
            "admin_login.html",
            {
                "request": request,
                "error": "An error occurred while loading the admin dashboard. Please try again."
            }
        )

# Home page
@app.get("/", response_class=HTMLResponse)
async def home(request: Request):
    current_user = await get_current_user_from_session(request)
    return templates.TemplateResponse(
        "index.html", 
        {"request": request, "current_user": current_user, "current_year": datetime.now().year}
    )

# Login page
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    return templates.TemplateResponse("login.html", {"request": request})

# Logout
@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/", status_code=303)
    response.delete_cookie(key="session_id")
    return response

# Search form
@app.get("/search", response_class=HTMLResponse)
async def search_form(request: Request):
    current_user = await get_current_user_from_session(request)
    if not current_user:
        return RedirectResponse(url="/login", status_code=303)
    
    # Read place types from the poi_searcher data folder in this repo
    place_types = []
    try:
        poi_data_dir = _PROJECT_ROOT / "poi_searcher" / "data"
        with open(poi_data_dir / "sorted_place_types_list.txt", "r") as f:
            place_types = [line.strip() for line in f if line.strip()]
    except Exception:
        place_types = []
    
    return templates.TemplateResponse(
        "search.html", 
        {"request": request, "current_user": current_user, "place_types": place_types}
    )

def process_place_ids_for_json_data(
    place_ids: List[str],
    proxies: List[str],
    username: str = Config.PROXY_USERNAME,
    password: str = Config.PROXY_PASSWORD,
    delay_range: Tuple[float, float] = (0.01, 0.2),
    cooldown_range: Tuple[float, float] = (30, 60)
) -> Dict[str, Dict[str, Any]]:
    """
    Process place IDs to extract JSON URLs, fetch JSON data, and extract opening_hours, latitude, longitude.
    Uses two-phase parallel processing: first get all JSON URLs, then get all JSON data.
    This matches the reference script's approach for maximum speed.
    
    Returns:
        Dict mapping place_id -> {json_url, opening_hours_str, latitude, longitude, place_address}
    """
    results = {}
    if not place_ids or not proxies:
        return results
    
    # Phase 1: Get all JSON URLs in parallel
    logger.info(f"Phase 1: Getting JSON URLs for {len(place_ids)} place IDs using {len(proxies)} proxies...")
    t_phase1_start = time.perf_counter()
    assignment: Dict[str, List[str]] = {p: [] for p in proxies}
    for idx, pid in enumerate(place_ids):
        proxy = proxies[idx % len(proxies)]
        assignment[proxy].append(pid)
    
    progress_counter = {"count": 0}
    progress_lock = threading.Lock()
    total = len(place_ids)
    json_url_map = {}  # place_id -> json_url
    
    def get_json_urls_batch(proxy: str, pids: List[str]) -> Dict[str, str]:
        """Phase 1: Get JSON URLs for assigned place IDs."""
        batch_results = {}
        extractor = LiveBusynessExtractor(username, password)
        extractor.proxies = [proxy]
        
        for pid in pids:
            success = False
            blocked = False
            json_url = None
            
            try:
                json_url = extractor.get_json_url(pid, proxy)
                success = bool(json_url)
                if not json_url:
                    blocked = True
            except Exception as e:
                logger.debug(f"Error getting JSON URL for {pid} with proxy {proxy}: {e}")
                blocked = True
            
            if json_url:
                batch_results[pid] = json_url
            
            # Update progress
            with progress_lock:
                progress_counter["count"] += 1
                current = progress_counter["count"]
            
            tick = "✅" if success else "❌"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{timestamp} - INFO - Phase 1: {pid}: {tick} {current}/{total}", 
                  end="\r" if current != total else "\n", flush=True)
            
            # Apply delays/cooldowns
            if blocked and not success:
                time.sleep(random.uniform(*cooldown_range))
            else:
                time.sleep(random.uniform(*delay_range))
        
        return batch_results
    
    # Phase 1: Get all JSON URLs
    with ThreadPoolExecutor(max_workers=len(proxies)) as executor:
        futures = []
        for proxy in proxies:
            pids = assignment[proxy]
            if not pids:
                continue
            future = executor.submit(get_json_urls_batch, proxy, pids)
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                batch_results = future.result()
                json_url_map.update(batch_results)
            except Exception as e:
                logger.error(f"Worker error in Phase 1: {e}")
    
    t_phase1_s = time.perf_counter() - t_phase1_start
    rate1 = (len(place_ids) / t_phase1_s) if t_phase1_s > 0 else 0.0
    logger.info(f"Phase 1 complete: Got {len(json_url_map)}/{len(place_ids)} JSON URLs in {t_phase1_s:.2f}s ({rate1:.1f} pid/s)")
    
    # Phase 2: Get JSON data for all URLs in parallel
    if not json_url_map:
        logger.warning("No JSON URLs obtained, skipping Phase 2")
        return results
    
    logger.info(f"Phase 2: Getting JSON data for {len(json_url_map)} place IDs using {len(proxies)} proxies...")
    t_phase2_start = time.perf_counter()
    
    # Assign (place_id, json_url) pairs to proxies
    url_pairs = [(pid, url) for pid, url in json_url_map.items()]
    assignment2: Dict[str, List[Tuple[str, str]]] = {p: [] for p in proxies}
    for idx, (pid, url) in enumerate(url_pairs):
        proxy = proxies[idx % len(proxies)]
        assignment2[proxy].append((pid, url))
    
    progress_counter2 = {"count": 0}
    
    def get_json_data_batch(proxy: str, pid_url_pairs: List[Tuple[str, str]]) -> Dict[str, Dict[str, Any]]:
        """Phase 2: Get JSON data and extract fields for assigned place IDs."""
        batch_results = {}
        extractor = LiveBusynessExtractor(username, password)
        extractor.proxies = [proxy]
        
        for pid, json_url in pid_url_pairs:
            opening_hours_str = None
            latitude = None
            longitude = None
            place_address = None
            success = False
            blocked = False
            
            try:
                json_data = extractor.get_json_data(json_url, proxy, pid)
                if json_data:
                    # Extract opening hours
                    opening_hours = extractor.extract_opening_hours(json_data) if hasattr(extractor, 'extract_opening_hours') else None
                    if opening_hours:
                        if isinstance(opening_hours, dict):
                            days_order = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
                            formatted = []
                            seen = set()
                            for day in days_order:
                                val = opening_hours.get(day)
                                if isinstance(val, list) and val:
                                    formatted.append(f"{day}: {'; '.join(val)}")
                                    seen.add(day)
                                elif isinstance(val, str) and val:
                                    formatted.append(f"{day}: {val}")
                                    seen.add(day)
                            for k, v in opening_hours.items():
                                if k in seen:
                                    continue
                                if isinstance(v, list) and v:
                                    formatted.append(f"{k}: {'; '.join(v)}")
                                elif isinstance(v, str) and v:
                                    formatted.append(f"{k}: {v}")
                            opening_hours_str = '; '.join(formatted) if formatted else None
                        elif isinstance(opening_hours, str):
                            opening_hours_str = opening_hours
                    
                    # Extract coordinates
                    location = extractor.extract_coordinates(json_data) if hasattr(extractor, 'extract_coordinates') else None
                    if isinstance(location, dict):
                        latitude = location.get('lat')
                        longitude = location.get('lng')
                    elif isinstance(location, (list, tuple)) and len(location) == 2:
                        latitude, longitude = location[0], location[1]
                    
                    # Extract place address
                    place_address = extractor.extract_place_address(json_data) if hasattr(extractor, 'extract_place_address') else None
                    success = True
                else:
                    blocked = True
            except Exception as e:
                logger.debug(f"Error getting JSON data for {pid} with proxy {proxy}: {e}")
                blocked = True
            
            # Store results
            batch_results[pid] = {
                'json_url': json_url,
                'opening_hours': opening_hours_str,
                'latitude': latitude,
                'longitude': longitude,
                'place_address': place_address
            }
            
            # Update progress
            with progress_lock:
                progress_counter2["count"] += 1
                current = progress_counter2["count"]
            
            tick = "✅" if success else "⚠️"
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            print(f"{timestamp} - INFO - Phase 2: {pid}: {tick} {current}/{len(url_pairs)}", 
                  end="\r" if current != len(url_pairs) else "\n", flush=True)
            
            # Apply delays/cooldowns
            if blocked and not success:
                time.sleep(random.uniform(*cooldown_range))
            else:
                time.sleep(random.uniform(*delay_range))
        
        return batch_results
    
    # Phase 2: Get all JSON data
    with ThreadPoolExecutor(max_workers=len(proxies)) as executor:
        futures = []
        for proxy in proxies:
            pairs = assignment2[proxy]
            if not pairs:
                continue
            future = executor.submit(get_json_data_batch, proxy, pairs)
            futures.append(future)
        
        for future in as_completed(futures):
            try:
                batch_results = future.result()
                results.update(batch_results)
            except Exception as e:
                logger.error(f"Worker error in Phase 2: {e}")
    
    t_phase2_s = time.perf_counter() - t_phase2_start
    rate2 = (len(json_url_map) / t_phase2_s) if t_phase2_s > 0 else 0.0
    try:
        with_opening = sum(1 for v in results.values() if v.get('opening_hours'))
        with_coords = sum(1 for v in results.values() if v.get('latitude') is not None and v.get('longitude') is not None)
        logger.info(
            f"Phase 2 complete: Extracted data for {len(results)} place IDs in {t_phase2_s:.2f}s ({rate2:.1f} pid/s); "
            f"with_opening_hours={with_opening}, with_coordinates={with_coords}"
        )
    except Exception:
        logger.info(f"Phase 2 complete: Extracted data for {len(results)} place IDs in {t_phase2_s:.2f}s ({rate2:.1f} pid/s)")
    logger.info(f"Total JSON pipeline time: {(t_phase1_s + t_phase2_s):.2f}s for {len(place_ids)} place IDs")
    return results

def filter_by_geometry(
    df: pd.DataFrame,
    location: str
) -> pd.DataFrame:
    """
    Filter DataFrame rows by checking if coordinates are within city geometry.
    Uses the same OSM polygon as the POI step, generated on the fly; nothing is saved.
    """
    try:
        # Build boundary via osmnx using the same location string
        try:
            import osmnx as ox
            gdf = ox.geocode_to_gdf(location)
            if gdf is None or gdf.empty:
                logger.info(f"OSM geometry not found for {location}, skipping geometry filter")
                return df
            gdf = gdf.explode(index_parts=False).reset_index(drop=True)
            if hasattr(gdf.geometry, 'make_valid'):
                gdf['geometry'] = gdf.geometry.make_valid()
            poly_union = gdf.geometry.unary_union if hasattr(gdf.geometry, 'unary_union') else gdf.geometry.iloc[0]
        except Exception as e:
            logger.info(f"OSM geometry fetch failed for {location}: {e}; skipping geometry filter")
            return df

        from shapely.geometry import Point
        filtered_indices = []
        for idx, row in df.iterrows():
            lat = row.get('latitude')
            lng = row.get('longitude')
            if lat is None or lng is None:
                continue
            try:
                pt = Point(float(lng), float(lat))
                if pt.within(poly_union) or pt.intersects(poly_union):
                    filtered_indices.append(idx)
            except Exception:
                continue

        if filtered_indices:
            filtered_df = df.loc[filtered_indices].copy()
            logger.info(f"Geometry filter: kept {len(filtered_df)}/{len(df)} rows inside {location} boundary")
            return filtered_df
        else:
            logger.warning(f"Geometry filter: no rows found inside {location} boundary")
            return df
    except Exception as e:
        logger.warning(f"Geometry filtering failed: {e}")
        return df

def save_place_ids_json(csv_file: str, output_dir: str) -> None:
    """Save JSON files for each place ID in the CSV file."""
    # Commented out to prevent saving JSON files
    pass
    """
    try:
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Read place IDs from CSV
        df = pd.read_csv(csv_file)
        if 'place_id' not in df.columns:
            logger.error("CSV file does not contain 'place_id' column")
            return
        
        # Initialize extractor
        extractor = LiveBusynessExtractor(Config.PROXY_USERNAME, Config.PROXY_PASSWORD)
        proxy_file = get_latest_proxy_file()
        extractor.load_proxies(proxy_file)
        
        # Process each place ID
        for place_id in df['place_id'].dropna().astype(str).unique():
            try:
                # Get proxy and JSON URL
                proxy = extractor.get_next_proxy(place_id)
                json_url = extractor.get_json_url(place_id, proxy)
                
                if not json_url:
                    logger.warning(f"Failed to get JSON URL for place_id {place_id}")
                    continue
                
                # Get JSON data
                json_data = extractor.get_json_data(json_url, proxy, place_id)
                if not json_data:
                    logger.warning(f"Failed to get JSON data for place_id {place_id}")
                    continue
                
                # Save JSON data
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"{place_id}_{timestamp}.json"
                filepath = os.path.join(output_dir, filename)
                
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump(json_data, f, ensure_ascii=False, indent=2)
                
                logger.info(f"Saved JSON data for place_id {place_id} to {filepath}")
                
            except Exception as e:
                logger.error(f"Error processing place_id {place_id}: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error in save_place_ids_json: {e}")
    """

@app.post("/run_search")
async def run_search(
    request: Request,
    locations: List[str] = Form(...),
    place_types: str = Form(...),
    start_date: str = Form(None),
    end_date: str = Form(None),
    only_place_ids: str = Form(None),
    background_tasks: BackgroundTasks = None
):
    try:
        current_user = await get_current_user_from_session(request)
        if not current_user:
            return RedirectResponse(url="/login", status_code=303)
        
        # Determine flags
        is_live_busyness = bool(start_date) and bool(end_date)
        only_place_ids_flag = (only_place_ids is not None)
        
        # Parse place types from comma-separated string
        place_types_list = [pt.strip() for pt in place_types.split(',') if pt.strip()]
        if not place_types_list:
            return templates.TemplateResponse(
                "search.html",
                {
                    "request": request,
                    "error": "No place types selected",
                    "place_types": pipeline_bridge.get_place_types()
                }
            )
        
        # Prepare a cumulative CSV to append results per-location (single canonical path)
        from pathlib import Path
        cumulative_dir = "/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/live_busyness_extraction/input"
        os.makedirs(cumulative_dir, exist_ok=True)
        cumulative_csv_path = os.path.join(cumulative_dir, f"poi_results_cumulative.csv")
        # Allocate a job_id up-front so we can tag rows as we append
        job_id = shortuuid.uuid()
        # Aggregate POI search results for all locations by appending to cumulative
        all_dfs = []
        # Clean old poi_searcher outputs before starting new runs
        try:
            poi_output_dir = "/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/poi_searcher/output"
            for fname in os.listdir(poi_output_dir):
                if fname.startswith("poi_results_") and fname.endswith(".csv"):
                    try:
                        os.remove(os.path.join(poi_output_dir, fname))
                    except Exception:
                        pass
        except Exception:
            pass
        for location in locations:
            output_csv = pipeline_bridge.run_poi_search(location, place_types_list)
            if not output_csv or not os.path.exists(output_csv) or os.path.getsize(output_csv) == 0:
                return templates.TemplateResponse(
                    "search.html",
                    {
                        "request": request,
                        "error": f"Failed to run POI search for {location}",
                        "place_types": pipeline_bridge.get_place_types()
                    }
                )
            try:
                df = pd.read_csv(output_csv)
                try:
                    total_rows = len(df)
                    if "place_id" in df.columns:
                        unique_pids = df["place_id"].astype(str).nunique()
                    elif "id" in df.columns:
                        unique_pids = df["id"].astype(str).nunique()
                    else:
                        unique_pids = total_rows
                    logger.info(f"POI CSV loaded: {output_csv} rows={total_rows}, unique_place_ids={unique_pids}")
                except Exception:
                    pass
                # Normalize place_id column
                if "place_id" not in df.columns and "id" not in df.columns:
                    return templates.TemplateResponse(
                        "search.html",
                        {
                            "request": request,
                            "error": f"CSV file for {location} does not contain place_id or id column",
                            "place_types": pipeline_bridge.get_place_types()
                        }
                    )
                if "place_id" not in df.columns and "id" in df.columns:
                    df["place_id"] = df["id"].astype(str)
                df["place_id"] = df["place_id"].astype(str)

                # Add required metadata columns for the shared cron pipeline
                # Normalize location string to first three comma-separated parts
                def _normalize_city_local(s: str) -> str:
                    try:
                        parts = [p.strip() for p in str(s).split(',') if p.strip()]
                        return ", ".join(parts[:3])
                    except Exception:
                        return str(s) if s is not None else ""
                df["city"] = _normalize_city_local(location)
                df["job_id"] = job_id
                # Only attach date fields for live busyness jobs
                if is_live_busyness:
                    df["start_date"] = start_date
                    df["end_date"] = end_date
                if "place_type" not in df.columns:
                    df["place_type"] = ",".join(place_types_list)
                
                # If only place IDs requested, output minimal CSV and skip JSON phases
                if only_place_ids_flag:
                    # Normalize columns
                    pid_col = 'place_id' if 'place_id' in df.columns else ('id' if 'id' in df.columns else None)
                    if pid_col is None:
                        raise Exception("POI CSV missing place_id/id column")
                    # Build two outputs:
                    # 1) Human-friendly per-location place_ids_<loc>.csv (place_id, place_type, location) for quick view
                    out_df_view = pd.DataFrame()
                    out_df_view['place_id'] = df[pid_col].astype(str)
                    out_df_view['place_type'] = df['place_type'] if 'place_type' in df.columns else ','.join(place_types_list)
                    out_df_view['location'] = location
                    # Write view file
                    import re, unicodedata
                    norm = unicodedata.normalize('NFKD', location)
                    ascii_loc = norm.encode('ascii', 'ignore').decode('ascii')
                    safe_loc = re.sub(r"\W+", "_", ascii_loc).strip('_')
                    out_path = os.path.join("/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/uploads", f"place_ids_{safe_loc}.csv")
                    os.makedirs(os.path.dirname(out_path), exist_ok=True)
                    out_df_view.to_csv(out_path, index=False)
                    logger.info(f"Only-place-ids output written: {out_path} (rows={len(out_df_view)})")
                    # 2) Minimal job rows (place_id, city, job_id) to be included in combined_df
                    minimal = pd.DataFrame()
                    minimal['place_id'] = df[pid_col].astype(str)
                    minimal['city'] = df.get('city', location)
                    minimal['job_id'] = job_id
                    all_dfs.append(minimal)
                # For live busyness jobs, extract JSON URLs, JSON data, opening hours, and coordinates
                elif is_live_busyness:
                    logger.info(f"Extracting JSON data for {len(df)} place IDs from {location}...")
                    
                    # Load proxies
                    try:
                        proxy_file = get_latest_proxy_file()
                        extractor = LiveBusynessExtractor(Config.PROXY_USERNAME, Config.PROXY_PASSWORD)
                        extractor.load_proxies(proxy_file)
                        proxies = extractor.proxies
                        logger.info(f"Loaded {len(proxies)} proxies for JSON extraction")
                    except Exception as e:
                        logger.error(f"Failed to load proxies: {e}")
                        proxies = []
                    
                    if proxies:
                        # Always process ALL place IDs (Phase 1 + Phase 2), then filter by geometry afterwards
                        place_ids_to_process = df['place_id'].tolist()
                        logger.info(f"Phase 0: Found {len(place_ids_to_process)} place IDs. Proceeding to Phase 1 (URLs) and Phase 2 (JSON data) for ALL IDs.")
                        
                        if place_ids_to_process:
                            # Process place IDs to get JSON data
                            json_data_map = process_place_ids_for_json_data(
                                place_ids_to_process,
                                proxies,
                                username=Config.PROXY_USERNAME,
                                password=Config.PROXY_PASSWORD
                            )
                   
                            # Update DataFrame with extracted data
                            for pid, data in json_data_map.items():
                                idx = df[df['place_id'] == pid].index
                                if len(idx) > 0:
                                    df.at[idx[0], 'json_url'] = data.get('json_url', '')
                                    if data.get('opening_hours'):
                                        df.at[idx[0], 'opening_hours'] = data.get('opening_hours')
                                    if data.get('latitude') is not None:
                                        df.at[idx[0], 'latitude'] = data.get('latitude')
                                    if data.get('longitude') is not None:
                                        df.at[idx[0], 'longitude'] = data.get('longitude')
                            
                            logger.info(f"Phase 2 result: Extracted JSON data for {len(json_data_map)}/{len(place_ids_to_process)} place IDs")
                        else:
                            logger.info("All place IDs already have json_url, skipping extraction")
                    
                    # Filter by geometry if coordinates are available
                    if 'latitude' in df.columns and 'longitude' in df.columns:
                        df = filter_by_geometry(df, location)
                
                # Normalize headers for opening hours
                if "openingHours" in df.columns and "opening_hours" not in df.columns:
                    df = df.rename(columns={"openingHours": "opening_hours"})
                if "Opening Hours" in df.columns and "opening_hours" not in df.columns:
                    df = df.rename(columns={"Opening Hours": "opening_hours"})
                
                # Ensure required columns exist
                for col in ['json_url', 'opening_hours', 'latitude', 'longitude']:
                    if col not in df.columns:
                        df[col] = ''
                
                rows_before_filter = len(df)
                all_dfs.append(df)
                # Only update the cumulative CSV for live-busyness jobs
                if is_live_busyness:
                    # De-duplicate against existing cumulative file and write back
                    df["place_id"] = df["place_id"].astype(str)
                    df["city"] = df["city"].astype(str)
                    df_new = df.drop_duplicates(subset=["place_id", "city"], keep="first")
                    # Drop legacy columns before merge/write
                    for _c in ["location", "geometry_class", "geometry_type", "address_type", "admin_level"]:
                        if _c in df_new.columns:
                            try:
                                df_new = df_new.drop(columns=[_c])
                            except Exception:
                                pass
                    # Ensure cumulative schema columns are present
                    for _c in ["start_date", "end_date"]:
                        if _c not in df_new.columns:
                            df_new[_c] = ""
                    if os.path.exists(cumulative_csv_path) and os.path.getsize(cumulative_csv_path) > 0:
                        try:
                            existing = pd.read_csv(cumulative_csv_path)
                            # Drop legacy columns from existing
                            for _c in ["location", "geometry_class", "geometry_type", "address_type", "admin_level"]:
                                if _c in existing.columns:
                                    try:
                                        existing = existing.drop(columns=[_c])
                                    except Exception:
                                        pass
                            if "place_id" in existing.columns and "city" in existing.columns:
                                existing["place_id"] = existing["place_id"].astype(str)
                                existing["city"] = existing["city"].astype(str)
                                merged = pd.concat([existing, df_new], ignore_index=True)
                                merged = merged.drop_duplicates(subset=["place_id", "city"], keep="first")
                                base_cols = ["place_id", "city", "job_id", "start_date", "end_date", "place_type"]
                                optional_cols = [c for c in ["json_url", "opening_hours", "latitude", "longitude"] if c in merged.columns]
                                columns_order = base_cols + optional_cols + [c for c in merged.columns if c not in set(base_cols + optional_cols)]
                                merged.to_csv(cumulative_csv_path, index=False, columns=columns_order)
                            else:
                                base_cols = ["place_id", "city", "job_id", "start_date", "end_date", "place_type"]
                                optional_cols = [c for c in ["json_url", "opening_hours", "latitude", "longitude"] if c in df_new.columns]
                                columns_order = base_cols + optional_cols + [c for c in df_new.columns if c not in set(base_cols + optional_cols)]
                                df_new.to_csv(cumulative_csv_path, mode='a', header=False, index=False, columns=columns_order)
                        except Exception:
                            base_cols = ["place_id", "city", "job_id", "start_date", "end_date", "place_type"]
                            optional_cols = [c for c in ["json_url", "opening_hours", "latitude", "longitude"] if c in df_new.columns]
                            columns_order = base_cols + optional_cols + [c for c in df_new.columns if c not in set(base_cols + optional_cols)]
                            df_new.to_csv(cumulative_csv_path, mode='a', header=False, index=False, columns=columns_order)
                    else:
                        base_cols = ["place_id", "city", "job_id", "start_date", "end_date", "place_type"]
                        optional_cols = [c for c in ["json_url", "opening_hours", "latitude", "longitude"] if c in df_new.columns]
                        columns_order = base_cols + optional_cols + [c for c in df_new.columns if c not in set(base_cols + optional_cols)]
                        df_new.to_csv(cumulative_csv_path, mode='w', header=True, index=False, columns=columns_order)
                    logger.info(f"Upserted {len(df_new)} rows for location '{location}' into {cumulative_csv_path}")
                # Remove per-location CSV only if we appended rows, keep empty for inspection
                if len(df) > 0:
                    try:
                        os.remove(output_csv)
                        logger.info(f"Removed location-specific CSV: {output_csv}")
                    except Exception as rm_err:
                        logger.warning(f"Could not remove location-specific CSV {output_csv}: {rm_err}")
                else:
                    logger.warning(f"No rows to append for location '{location}'. Keeping {output_csv} for inspection.")
            except Exception as e:
                logger.error(f"Error reading CSV file for {location}: {e}")
                return templates.TemplateResponse(
                    "search.html",
                    {
                        "request": request,
                        "error": f"Failed to read CSV file for {location}: {str(e)}",
                        "place_types": pipeline_bridge.get_place_types()
                    }
                )
        # Concatenate all results
        if not all_dfs:
            return templates.TemplateResponse(
                "search.html",
                {
                    "request": request,
                    "error": "No results found for any location.",
                    "place_types": pipeline_bridge.get_place_types()
                }
            )
        combined_df = pd.concat(all_dfs, ignore_index=True)
        try:
            total_rows = len(combined_df)
            uniq_place_ids = combined_df['place_id'].astype(str).nunique() if 'place_id' in combined_df.columns else total_rows
            with_json_url = combined_df['json_url'].notna().sum() if 'json_url' in combined_df.columns else 0
            with_opening = combined_df['opening_hours'].notna().sum() if 'opening_hours' in combined_df.columns else 0
            logger.info(
                f"Aggregate before dedupe: rows={total_rows}, unique_place_ids={uniq_place_ids}, "
                f"with_json_url={with_json_url}, with_opening_hours={with_opening}"
            )
        except Exception:
            pass
        # Keep full columns when only_place_ids is requested; otherwise drop geometry/legacy columns
        if not only_place_ids_flag:
            cols_to_drop = ["location", "geometry_class", "geometry_type", "address_type", "admin_level"]
            try:
                combined_df = combined_df.drop(columns=[c for c in cols_to_drop if c in combined_df.columns])
            except Exception:
                pass
        # Save per-job results to a new CSV
        output_dir = os.path.join("/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/uploads", f"job_{job_id}_results")
        os.makedirs(output_dir, exist_ok=True)
        job_input_file = os.path.join("/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/uploads", f"job_{job_id}.csv")
        # Ensure place_id uniqueness before writing per-job files
        try:
            if "place_id" in combined_df.columns:
                combined_df["place_id"] = combined_df["place_id"].astype(str)
                combined_df = combined_df.drop_duplicates(subset=["place_id"], keep="first").reset_index(drop=True)
                logger.info(f"Final for cron: unique_place_ids={len(combined_df)}")
        except Exception:
            pass

        if is_live_busyness:
            # Keep full dataframe when live busyness is requested
            combined_df.to_csv(job_input_file, index=False)
        elif only_place_ids_flag:
            # Minimal per-job output when only place IDs are requested
            minimal_cols = ["place_id", "city", "job_id"]
            for col in ["place_id", "city", "job_id"]:
                if col not in combined_df.columns:
                    combined_df[col] = ""
            minimal_df = combined_df[minimal_cols].copy()
            minimal_df.to_csv(job_input_file, index=False)
        else:
            # Produce simplified CSV for straight jobs
            simple_cols = ["place_id", "city", "job_id", "place_type", "latitude", "longitude"]
            for col in ["latitude", "longitude"]:
                if col not in combined_df.columns:
                    combined_df[col] = ""
            for col in ["city", "job_id", "place_type"]:
                if col not in combined_df.columns:
                    combined_df[col] = ""
            simple_df = combined_df[[c for c in simple_cols if c in combined_df.columns]].copy()
            for c in simple_cols:
                if c not in simple_df.columns:
                    simple_df[c] = ""
            simple_df = simple_df[simple_cols]
            simple_df.to_csv(job_input_file, index=False)
        if is_live_busyness:
            # Maintain ONLY ONE shared cron/script that reads the cumulative CSV
            script_dir = os.path.join("/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/scripts")
            os.makedirs(script_dir, exist_ok=True)
            shared_script = os.path.join(script_dir, "live_busyness_shared.sh")

            # Do not modify or remove any existing scripts; only ensure the shared script exists

            # Create/overwrite the shared script to read the cumulative file (canonical path)
            with open(shared_script, "w") as f:
                f.write(f"""#!/bin/bash

LOG_HELPER="/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/scripts/log_to_parquet.py"
LOCK_FILE="/tmp/live_busyness_shared.lock"

log_to_parquet() {{
    echo "$1" | python3 "$LOG_HELPER" --level "$2" 2>&1 || echo "Failed to log: $1" >&2
}}

# Only forward cron-relevant lines to parquet to avoid per-place noise
filter_and_forward() {{
    local line="$1"
    if [[ "$line" == *"ERROR"* || "$line" == *"Traceback"* || "$line" == *"Exception"* ]]; then
        log_to_parquet "$line" "ERROR"
        return
    fi
    if [[ "$line" == *"Working for "* && "$line" == *"open place ids out of"* ]]; then
        log_to_parquet "$line" "INFO"
        return
    fi
    if [[ "$line" == *"Opening-hours filter:"* ]]; then
        log_to_parquet "$line" "INFO"
        return
    fi
    if [[ "$line" == *"Processing"* && "$line" == *"place_id row(s) for JSON-response pipeline"* ]]; then
        log_to_parquet "$line" "INFO"
        return
    fi
    if [[ "$line" == *"Detected city column."* ]]; then
        log_to_parquet "$line" "INFO"
        return
    fi
    # drop everything else to keep cron logs compact
}}

# Check if another instance is already running
if [ -f "$LOCK_FILE" ]; then
    PID=$(cat "$LOCK_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        log_to_parquet "Another instance is already running (PID: $PID). Skipping this run at $(date -u)" "WARNING"
        exit 0
    else
        # Stale lock file, remove it
        rm -f "$LOCK_FILE"
    fi
fi

# Create lock file
echo $$ > "$LOCK_FILE"
trap "rm -f '$LOCK_FILE'" EXIT

log_to_parquet "Starting shared busyness extraction at $(date -u) (PID: $$)" "INFO"
cd /home/hewanshrestha/Desktop/i2sc/testing_monitoring_system || {{
    log_to_parquet "ERROR: Failed to change directory" "ERROR"
    exit 1
}}
export PYTHONPATH=/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system
export PATH=/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/monitor_sys/bin:$PATH
export TZ=UTC

CMD_PY=$(which python3)
log_to_parquet "Running shared: $CMD_PY live_busyness_extraction/main.py --input_file {cumulative_csv_path} --output_file /home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/live_busyness_extraction/output/placeholder.json" "INFO"

# Run Python script and capture output, writing to parquet in real-time
log_to_parquet "Python script started, capturing output..." "INFO"
$CMD_PY \
  /home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/live_busyness_extraction/main.py \
  --proxy_username {Config.PROXY_USERNAME} \
  --proxy_password {Config.PROXY_PASSWORD} \
  --input_file "{cumulative_csv_path}" \
  --output_file "/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/live_busyness_extraction/output/placeholder.json" 2>&1 | while IFS= read -r line; do
    filter_and_forward "$line"
done
EXIT_CODE=${{PIPESTATUS[0]}}

log_to_parquet "Python script finished with exit code: $EXIT_CODE" "INFO"

if [ $EXIT_CODE -eq 0 ]; then
  log_to_parquet "Shared busyness extraction completed successfully at $(date -u)" "INFO"
else
  log_to_parquet "Shared busyness extraction failed at $(date -u)" "ERROR"
fi

log_to_parquet "=======================================================" "INFO"
""")
            os.chmod(shared_script, 0o755)

            # Append a static shared cronjob if not already present; do not modify/remove existing cronjobs
            try:
                result = subprocess.run(["crontab", "-l"], capture_output=True, text=True)
                current_cron = result.stdout if result.returncode == 0 else ""
                cron_line = f"0 * * * * {shared_script}"
                if cron_line not in current_cron and shared_script not in current_cron:
                    new_cron = (current_cron.rstrip("\n") + "\n" + cron_line + "\n").lstrip("\n")
                    p = subprocess.run(["crontab", "-"], input=new_cron, text=True, capture_output=True)
                    if p.returncode != 0:
                        raise RuntimeError(p.stderr.strip() or "Failed to write crontab")
            except Exception as _cron_err:
                logger.warning(f"Skipping cron registration (non-fatal): {_cron_err}")
            job_data = {
                "id": job_id,
                "username": current_user["username"],
                "status": "PENDING",
                "timestamp": datetime.now(),
                "last_updated": datetime.now(),
                "file_path": job_input_file,
                "output_file": None,
                "total_place_ids": len(combined_df),
                "processed_place_ids": 0,
                "extraction_options": {
                    "locations": locations,
                    "place_types": place_types_list,
                    "live_busyness": True,
                    "start_date": start_date,
                    "end_date": end_date
                },
                "error_message": None
            }
        else:
            final_csv_path = os.path.join(output_dir, "extraction_results.csv")
            shutil.copy2(job_input_file, final_csv_path)
            zip_path = os.path.join(output_dir, "results.zip")
            with zipfile.ZipFile(zip_path, "w") as zipf:
                zipf.write(final_csv_path, "extraction_results.csv")
            job_data = {
                "id": job_id,
                "username": current_user["username"],
                "status": "COMPLETED",
                "timestamp": datetime.now(),
                "last_updated": datetime.now(),
                "file_path": job_input_file,
                "output_file": zip_path,
                "total_place_ids": len(combined_df),
                "processed_place_ids": len(combined_df),
                "extraction_options": {
                    "locations": locations,
                    "place_types": place_types_list
                },
                "error_message": None
            }
        query = jobs.insert().values(**job_data)
        await database.execute(query)
        memory_job_data = {
            "job_id": job_id,
            "status": job_data["status"],
            "timestamp": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "last_updated": datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            "api_key": current_user["api_key"],
            "canceled": False,
            "pending_place_ids": combined_df["place_id"].tolist() if "place_id" in combined_df.columns else combined_df["id"].tolist() if is_live_busyness else [],
            "processed_place_ids": [] if is_live_busyness else (combined_df["place_id"].tolist() if "place_id" in combined_df.columns else combined_df["id"].tolist()),
            "total_place_ids": len(combined_df),
            "file_path": job_input_file,
            "output_file": job_data["output_file"],
            "extraction_options": job_data["extraction_options"]
        }
        JOBS[job_id] = memory_job_data
        # After job data persisted, delete poi_searcher/output to keep workspace clean
        try:
            poi_output_dir = "/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/poi_searcher/output"
            import shutil as _shutil
            if os.path.isdir(poi_output_dir):
                _shutil.rmtree(poi_output_dir, ignore_errors=True)
        except Exception:
            pass
        if not is_live_busyness:
            try:
                from app.services.job_manager import cleanup_input_file_for_completed_job
                cleanup_success = cleanup_input_file_for_completed_job(job_id)
                if cleanup_success:
                    logger.info(f"Cleaned up input file for completed job {job_id}")
                else:
                    logger.warning(f"Failed to clean up input file for completed job {job_id}")
            except Exception as cleanup_error:
                logger.error(f"Error during input file cleanup for job {job_id}: {str(cleanup_error)}")
        return RedirectResponse(url=f"/job_status/{job_id}", status_code=303)
    except Exception as e:
        logger.error(f"Error in run_search: {e}")
        return templates.TemplateResponse(
            "search.html",
            {
                "request": request,
                "error": f"An error occurred: {str(e)}",
                "place_types": pipeline_bridge.get_place_types()
            }
        )

# Jobs list
@app.get("/jobs", response_class=HTMLResponse)
async def jobs_list(request: Request):
    current_user = await get_current_user_from_session(request)
    if not current_user:
        return RedirectResponse(url="/login", status_code=303)
    
    all_jobs = await list_all_jobs()
    user_jobs = {job_id: job for job_id, job in all_jobs.items() 
                if job["username"] == current_user["username"]}
    
    # Format timestamps for each job to match template's expected format
    for job in user_jobs.values():
        if isinstance(job.get("timestamp"), datetime):
            # Format to YYYY-MM-DDTHH:MM:SS
            job["timestamp"] = job["timestamp"].strftime("%Y-%m-%dT%H:%M:%S")
    
    return templates.TemplateResponse(
        "jobs.html", 
        {"request": request, "current_user": current_user, "jobs": user_jobs}
    )

# Job status
@app.get("/job_status/{job_id}", response_class=HTMLResponse)
async def job_status_page(request: Request, job_id: str):
    current_user = await get_current_user_from_session(request)
    if not current_user or "username" not in current_user:
        return RedirectResponse(url="/login", status_code=303)
    
    try:
        # First check in-memory jobs
        if job_id in JOBS:
            job_metadata = JOBS[job_id]
            job_metadata["job_id"] = job_id
            
            # Check if output file exists and update status if needed
            output_dir = os.path.join("/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/uploads", f"job_{job_id}_results")
            zip_path = os.path.join(output_dir, "results.zip")
            if os.path.exists(zip_path) and job_metadata["status"] == "PENDING":
                # Update both database and memory status
                update_query = jobs.update().where(jobs.c.id == job_id).values(
                    status="COMPLETED",
                    last_updated=datetime.now(),
                    output_file=zip_path
                )
                await database.execute(update_query)
                job_metadata["status"] = "COMPLETED"
                job_metadata["output_file"] = zip_path
                
                # Clean up input file for completed job
                try:
                    from app.services.job_manager import cleanup_input_file_for_completed_job
                    cleanup_success = cleanup_input_file_for_completed_job(job_id)
                    if cleanup_success:
                        logger.info(f"Cleaned up input file for completed job {job_id} during status check")
                    else:
                        logger.warning(f"Failed to clean up input file for completed job {job_id} during status check")
                except Exception as cleanup_error:
                    logger.error(f"Error during input file cleanup for job {job_id} during status check: {str(cleanup_error)}")
            
            # Format timestamp for template
            if isinstance(job_metadata.get("timestamp"), datetime):
                job_metadata["timestamp"] = job_metadata["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
            elif isinstance(job_metadata.get("timestamp"), str):
                try:
                    dt = datetime.fromisoformat(job_metadata["timestamp"])
                    job_metadata["timestamp"] = dt.strftime("%Y-%m-%d %H:%M:%S")
                except ValueError:
                    pass
            
            return templates.TemplateResponse(
                "job_status.html", 
                {"request": request, "current_user": current_user, "job": job_metadata}
            )
        
        # If not in memory, check database
        job_metadata = await get_job_status(job_id)
        if not job_metadata:
            return templates.TemplateResponse(
                "jobs.html", 
                {
                    "request": request, 
                    "current_user": current_user,
                    "error": "Job not found or invalid"
                }
            )
        
        # Check if username exists in job metadata
        if "username" not in job_metadata:
            logger.error(f"Job {job_id} missing username field")
            return templates.TemplateResponse(
                "jobs.html", 
                {
                    "request": request, 
                    "current_user": current_user,
                    "error": "Invalid job data"
                }
            )
        
        if job_metadata["username"] != current_user["username"]:
            return templates.TemplateResponse(
                "jobs.html", 
                {
                    "request": request, 
                    "current_user": current_user,
                    "error": "Not authorized to view this job"
                }
            )
        
        # Check if output file exists and update status if needed
        output_dir = os.path.join("/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/uploads", f"job_{job_id}_results")
        zip_path = os.path.join(output_dir, "results.zip")
        if os.path.exists(zip_path) and job_metadata["status"] == "PENDING":
            # Update both database and memory status
            update_query = jobs.update().where(jobs.c.id == job_id).values(
                status="COMPLETED",
                last_updated=datetime.now(),
                output_file=zip_path
            )
            await database.execute(update_query)
            job_metadata["status"] = "COMPLETED"
            job_metadata["output_file"] = zip_path
            
            # Clean up input file for completed job
            try:
                from app.services.job_manager import cleanup_input_file_for_completed_job
                cleanup_success = cleanup_input_file_for_completed_job(job_id)
                if cleanup_success:
                    logger.info(f"Cleaned up input file for completed job {job_id} during status check")
                else:
                    logger.warning(f"Failed to clean up input file for completed job {job_id} during status check")
            except Exception as cleanup_error:
                logger.error(f"Error during input file cleanup for job {job_id} during status check: {str(cleanup_error)}")
            
            # Update in-memory data
            if job_id not in JOBS:
                JOBS[job_id] = job_metadata
            else:
                JOBS[job_id].update(job_metadata)
        
        # Format timestamp for template
        if isinstance(job_metadata.get("timestamp"), datetime):
            job_metadata["timestamp"] = job_metadata["timestamp"].strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(job_metadata.get("timestamp"), str):
            try:
                dt = datetime.fromisoformat(job_metadata["timestamp"])
                job_metadata["timestamp"] = dt.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                pass
        
        job_metadata["job_id"] = job_id
        
        return templates.TemplateResponse(
            "job_status.html", 
            {"request": request, "current_user": current_user, "job": job_metadata}
        )
    except Exception as e:
        logger.error(f"Error in job_status_page: {str(e)}")
        return templates.TemplateResponse(
            "jobs.html", 
            {
                "request": request, 
                "current_user": current_user,
                "error": f"An error occurred: {str(e)}"
            }
        )

@app.get("/job_results/{job_id}")
async def job_results(job_id: str, request: Request):
    current_user = await get_current_user_from_session(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        # First check in-memory jobs
        if job_id in JOBS:
            job_metadata = JOBS[job_id]
            if job_metadata["api_key"] != current_user["api_key"]:
                raise HTTPException(status_code=403, detail="Unauthorized access.")
            
            # Check if output file exists with correct checkpoint path
            output_dir = os.path.join("/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/uploads", f"job_{job_id}_results")
            zip_path = os.path.join(output_dir, "results.zip")
            
            logger.info(f"Checking for zip file at: {zip_path}")
            logger.info(f"Job status: {job_metadata['status']}")
            
            if os.path.exists(zip_path):
                logger.info(f"Found zip file at: {zip_path}")
                return FileResponse(
                    path=zip_path,
                    filename=f"job_{job_id}_results.zip",
                    media_type="application/zip"
                )
            else:
                logger.error(f"Zip file not found at: {zip_path}")
        
        # If not in memory, check database
        query = jobs.select().where(jobs.c.id == job_id)
        job = await database.fetch_one(query)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found.")
        
        if job["username"] != current_user["username"]:
            raise HTTPException(status_code=403, detail="Unauthorized access.")
        
        # Check if output file exists with correct checkpoint path
        output_dir = os.path.join("/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/uploads", f"job_{job_id}_results")
        zip_path = os.path.join(output_dir, "results.zip")
        
        logger.info(f"Checking for zip file at: {zip_path}")
        logger.info(f"Job status: {job['status']}")
        
        if os.path.exists(zip_path):
            logger.info(f"Found zip file at: {zip_path}")
            return FileResponse(
                path=zip_path,
                filename=f"job_{job_id}_results.zip",
                media_type="application/zip"
            )
        else:
            logger.error(f"Zip file not found at: {zip_path}")
            
            # Check if the output directory exists
            if os.path.exists(output_dir):
                logger.info(f"Output directory exists at: {output_dir}")
                # List contents of the directory
                files = os.listdir(output_dir)
                logger.info(f"Files in output directory: {files}")
            else:
                logger.error(f"Output directory not found at: {output_dir}")
        
        raise HTTPException(status_code=400, detail="Job not complete.")
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in job_results: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/cancel_job/{job_id}")
async def cancel_job_endpoint(
    job_id: str,
    request: Request,
    current_user=Depends(get_current_user_from_session)
):
    if not current_user:
        return JSONResponse({"status": "error", "message": "Not authenticated"}, status_code=401)

    success, message = await cancel_job(job_id, api_key=current_user["api_key"])
    if not success:
        return JSONResponse({"status": "error", "message": message}, status_code=400)

    return {"status": "success", "message": "Job canceled successfully"}

# Add this temporary endpoint (remove it after use)
@app.post("/admin/clear-data")
async def clear_data_endpoint(current_user=Depends(get_admin_user)):
    if clear_redis_data():
        return {"status": "success", "message": "Redis data cleared successfully"}
    return {"status": "error", "message": "Failed to clear Redis data"}

@app.post("/admin/cancel-job/{job_id}")
async def admin_cancel_job(job_id: str, current_user=Depends(get_admin_user)):
    try:
        success, message = await cancel_job(job_id, api_key=None)  # Pass None to bypass API key check for admin
        if success:
            return {"status": "success", "message": "Job cancelled successfully"}
        else:
            raise HTTPException(status_code=400, detail=message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error cancelling job: {str(e)}")

@app.post("/api/jobs")
async def create_job_api(request: JobRequest, background_tasks: BackgroundTasks):
    """Create a new job for busyness extraction."""
    try:
        # Create a temporary CSV file with place IDs
        job_id = shortuuid.uuid()
        csv_path = os.path.join(UPLOAD_DIR, f"job_{job_id}.csv")
        
        # Create DataFrame with place IDs and types
        df = pd.DataFrame({
            'place_id': request.place_ids,
            'place_type': request.place_types
        })
        df.to_csv(csv_path, index=False)
        
        # Create job in database
        job_data = await create_job(None, csv_path)  # None for API key since this is an API endpoint
        if not job_data:
            raise HTTPException(status_code=500, detail="Failed to create job")
        
        # Add job processing to background tasks
        background_tasks.add_task(process_job, job_data["job_id"])
        
        return {"job_id": job_data["job_id"], "status": "processing"}
        
    except Exception as e:
        logger.error(f"Error creating job: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/jobs/{job_id}")
async def get_job_status_api(job_id: str):
    """Get the status of a job."""
    try:
        job_metadata = await get_job_status(job_id)
        if not job_metadata:
            raise HTTPException(status_code=404, detail="Job not found")
            
        return {
            "job_id": job_id,
            "status": job_metadata["status"],
            "output_file": job_metadata.get("output_file")
        }
            
    except Exception as e:
        logger.error(f"Error getting job status: {e}")
        raise HTTPException(status_code=500, detail=str(e))

def get_latest_proxy_file() -> str:
    """Get the most recent proxy file from the working_proxies directory."""
    proxy_files = glob.glob('/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/live_busyness_extraction/input/working_proxies/working_proxies_*.txt')
    if not proxy_files:
        raise FileNotFoundError("No proxy files found in working_proxies directory")
    return max(proxy_files, key=os.path.getctime)

@app.get("/api/intermediate_results/{job_id}")
async def get_intermediate_results(job_id: str, request: Request):
    """Get all intermediate results for a specific job as a zip file containing all output files."""
    current_user = await get_current_user_from_session(request)
    if not current_user:
        raise HTTPException(status_code=401, detail="Not authenticated")
    
    try:
        # Check if user has access to this job
        query = jobs.select().where(jobs.c.id == job_id)
        job = await database.fetch_one(query)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        
        if job["username"] != current_user["username"]:
            raise HTTPException(status_code=403, detail="Unauthorized access")
        
        # Construct the path to the output directory
        output_dir = os.path.join("/home/hewanshrestha/Desktop/i2sc/testing_monitoring_system/api_json_extraction/uploads", f"job_{job_id}_results")
        
        if not os.path.exists(output_dir):
            raise HTTPException(status_code=404, detail="Results directory not found")
        
        # Create a temporary zip file in memory
        zip_buffer = io.BytesIO()
        
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
            # Add all files from the output directory to the zip
            for root, dirs, files in os.walk(output_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    # Calculate the relative path for the zip
                    relative_path = os.path.relpath(file_path, output_dir)
                    zipf.write(file_path, relative_path)
        
        # Reset buffer position
        zip_buffer.seek(0)
        
        # Return the zip file as a streaming response
        return StreamingResponse(
            io.BytesIO(zip_buffer.getvalue()),
            media_type="application/zip",
            headers={"Content-Disposition": f"attachment; filename=intermediate_results_{job_id}.zip"}
        )
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error in get_intermediate_results: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/admin/manual-cleanup/{job_id}")
async def manual_cleanup_job_endpoint(job_id: str, current_user=Depends(get_admin_user)):
    """Manual cleanup endpoint for stuck jobs."""
    try:
        from app.services.job_manager import manual_cleanup_job
        success, message = await manual_cleanup_job(job_id)
        if success:
            return {"status": "success", "message": message}
        else:
            raise HTTPException(status_code=400, detail=message)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error during manual cleanup: {str(e)}")
