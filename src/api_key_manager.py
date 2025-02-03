import asyncio
import time
from collections import deque
import logging

class KeyManager:
    def __init__(self, api_keys, cooldown_time=60):
        self.keys = deque(api_keys)
        self.cooldowns = {}
        self.cooldown_time = cooldown_time
        self.api_requests_count = {key: 0 for key in api_keys}

    async def get_key(self):
        while True:
            now = time.time()
            # Check each key in order to find the first available
            for i in range(len(self.keys)):
                key = self.keys[i]
                if now - self.cooldowns.get(key, 0) >= self.cooldown_time:
                    # Rotate the deque so next key is checked first next time
                    self.keys.rotate(-i-1)
                    return key
            
            # Calculate earliest available time if no keys are ready
            earliest_time = min(
                (self.cooldowns.get(k, 0) + self.cooldown_time for k in self.keys),
                default=None
            )
            if earliest_time:
                sleep_time = max(earliest_time - now, 0)
                logging.info(f"All keys on cooldown. Waiting {sleep_time:.1f} seconds...")
                await asyncio.sleep(sleep_time)
            else:
                logging.info("No keys available. Waiting...")
                await asyncio.sleep(self.cooldown_time)

    def mark_rate_limit(self, key):
        self.cooldowns[key] = time.time()
        logging.info(f"Key {key[-8:]} rate limited")