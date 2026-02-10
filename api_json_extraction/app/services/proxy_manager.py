import redis
from config.config import Config
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

redis_client = redis.Redis(host=Config.REDIS_HOST, port=Config.REDIS_PORT, decode_responses=True)

def cleanup_redis_state():
    """Clean up Redis state by releasing all proxies and removing old job data."""
    try:
        # Get all proxies in use
        proxies_in_use = redis_client.smembers("proxies_in_use")
        
        # Release all proxies back to the available pool
        for proxy in proxies_in_use:
            redis_client.srem("proxies_in_use", proxy)
            redis_client.sadd("proxies", proxy)
            logger.info(f"Released proxy {proxy} during cleanup")
        
        # Remove all job-related keys
        job_keys = redis_client.keys("job:*")
        if job_keys:
            redis_client.delete(*job_keys)
            logger.info(f"Cleaned up {len(job_keys)} job-related keys")
        
        # Remove proxy health data
        redis_client.delete("proxy_health_status")
        redis_client.delete("proxy_health_history")
        
        logger.info("Successfully cleaned up Redis state")
        return True
    except Exception as e:
        logger.error(f"Error cleaning up Redis state: {e}")
        return False

def load_proxies_from_file(file_path: str = None):
    try:
        # Determine path priority: explicit arg > Config.PROXY_FILE > default inside package
        if not file_path:
            file_path = getattr(Config, "PROXY_FILE", None)
        if not file_path or not isinstance(file_path, str):
            file_path = "data/proxies.txt"
        # If not absolute, resolve relative to the api_json_extraction directory
        if not (file_path.startswith("/") or ":\\" in file_path):
            import os as _os
            _base_dir = _os.path.dirname(_os.path.dirname(__file__))
            file_path = _os.path.join(_base_dir, file_path)

        logger.info(f"Loading proxies from {file_path}")
        # Clean up Redis state before loading new proxies
        cleanup_redis_state()
        
        with open(file_path, "r") as file:
            proxies = [line.strip() for line in file if line.strip()]
            for proxy in proxies:
                redis_client.sadd("proxies", proxy)
        logger.info(f"Successfully loaded {len(proxies)} proxies from {file_path}")
        return len(proxies)
    except Exception as e:
        logger.error(f"Error loading proxies: {e}")
        return 0

def get_proxy():
    proxy = redis_client.srandmember("proxies")
    if proxy:
        redis_client.srem("proxies", proxy)
        redis_client.sadd("proxies_in_use", proxy)
        logger.info(f"Assigned proxy: {proxy}")
        return proxy
    logger.warning("No proxies available")
    return None

def release_proxy(proxy: str) -> bool:
    """
    Release a proxy back into the Redis set.
    Args:
        proxy (str): The proxy string to release.
    Returns:
        bool: True if the proxy was successfully released, False otherwise.
    """
    try:
        # Check if the proxy is in the 'proxies_in_use' set
        if redis_client.sismember("proxies_in_use", proxy):
            # Remove the proxy from the 'proxies_in_use' set
            redis_client.srem("proxies_in_use", proxy)
            # Add the proxy back to the 'proxies' set
            redis_client.sadd("proxies", proxy)
            logger.info(f"Proxy {proxy} released successfully.")
            return True
        else:
            # Log an error if the proxy is not in use
            logger.warning(f"Proxy {proxy} is not in use or is invalid.")
            return False
    except Exception as e:
        logger.error(f"Error releasing proxy {proxy}: {e}")
        return False

def list_all_proxies():
    proxies = list(redis_client.smembers("proxies"))
    logger.info(f"Retrieved {len(proxies)} proxies from Redis")
    return proxies