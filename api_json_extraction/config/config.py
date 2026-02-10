from dotenv import load_dotenv
import os
from pathlib import Path

# Load .env from project root, resolved relative to this file so it works
# regardless of where the repo is checked out.
_CONFIG_DIR = Path(__file__).resolve().parent
_PROJECT_ROOT = _CONFIG_DIR.parents[2]  # .../testing_monitoring_system_remote
load_dotenv(os.path.join(_PROJECT_ROOT, ".env"))

LOGS_DIR = "./logs"
os.makedirs(LOGS_DIR, exist_ok=True)

class Config:
    # Resolve database path inside the api_json_extraction folder to avoid writing in project root
    _BASE_DIR = os.path.dirname(os.path.dirname(__file__))
    _DEFAULT_DB_PATH = os.path.join(_BASE_DIR, "test.db")
    DATABASE_URL = os.getenv("DATABASE_URL", f"sqlite:///{_DEFAULT_DB_PATH}")
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
    LOG_FILE_PATH = os.path.join(LOGS_DIR, "general_logs.log")
    
    # Proxy configuration
    PROXY_USERNAME = os.getenv("PROXY_USERNAME")
    PROXY_PASSWORD = os.getenv("PROXY_PASSWORD")
    PROXY_FILE = os.getenv(
        "PROXY_FILE",
        os.path.join(str(_PROJECT_ROOT), "api_json_extraction", "data", "proxies.txt"),
    )