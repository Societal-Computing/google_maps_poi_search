import asyncio
import time
from collections import deque
import logging
import json

logger = logging.getLogger(__name__)

class APIKeyManager:
    """
    Manages a pool of API keys, handling rotation, rate limits, and request tracking.
    """
    def __init__(self, config, api_keys, cooldown_time, api_requests_count):
        """
        Initializes the APIKeyManager.
        
        Args:
            config (dict): Configuration settings, including paths for saving request data.
            api_keys (list): List of API keys.
            cooldown_time (int): Cooldown duration (in seconds) before a key can be reused.
            api_requests_count (dict): Dictionary tracking API request counts for each key.
        """
        self.config = config
        self.keys = deque(api_keys)
        self.cooldown_time = cooldown_time
        self.cooldowns = {}
        self.api_requests_count = api_requests_count

    async def get_key(self):
        """
        Retrieves an available API key that is not on cooldown.

        Returns:
            str: An API key that is ready for use.
        
        If all keys are on cooldown, the function waits before retrying.
        """
        while True:
            available = [k for k in self.keys if time.time() - self.cooldowns.get(k, 0) >= self.cooldown_time]
            if available:
                self.keys.rotate(-1)
                return available[0]
            logger.info("All keys on cooldown. Waiting...")
            await asyncio.sleep(self.cooldown_time)

    def mark_rate_limit(self, key):
        """
        Marks an API key as rate-limited and starts its cooldown period.

        Args:
            key (str): The API key to be marked as rate-limited.
        """
        self.cooldowns[key] = time.time()
        logger.info(f"Key {key[-8:]} rate limited")

    def save_api_requests(self):
        """
        Saves the API request counts to a JSON file.
        
        The file path is retrieved from the configuration settings.
        """
        path = self.config['paths']['api_requests_json']
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.api_requests_count, f, indent=4)
        logging.info(f"Saved API requests count to {path}")

