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
            available = [k for k in self.keys if now - self.cooldowns.get(k, 0) >= self.cooldown_time]
            if available:
                self.keys.rotate(-1)
                return available[0]
            logging.info("All keys on cooldown. Waiting...")
            await asyncio.sleep(self.cooldown_time)

    def mark_rate_limit(self, key):
        self.cooldowns[key] = time.time()
        logging.info(f"Key {key[-8:]} rate limited")