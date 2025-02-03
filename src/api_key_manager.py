import asyncio
import time
from collections import deque
import logging

class KeyManager:
    """
    A class to manage API keys with rate-limiting control.

    The KeyManager ensures that API keys are used in a round-robin fashion,
    taking cooldown periods into account. If all keys are rate-limited,
    it will wait until one becomes available before using it again.
    """

    def __init__(self, api_keys, cooldown_time):
        """
        Initialize the KeyManager.

        Args:
            api_keys (list): A list of API keys to be managed.
            cooldown_time (int): The cooldown time (in seconds) after an API key is used.
        """
        self.keys = deque(api_keys)  # Deque for round-robin key rotation
        self.cooldowns = {}  # Dictionary to store the last cooldown time of each key
        self.cooldown_time = cooldown_time  # The cooldown time (in seconds) for each key
        self.api_requests_count = {key: 0 for key in api_keys}  # Tracks how many times each key has been used

    async def get_key(self):
        """
        Asynchronously fetch an API key that is available to use (i.e., not rate-limited).

        This function rotates through the available keys, checking their cooldown
        times. If all keys are rate-limited, it waits until one becomes available.
        
        Returns:
            str: The first available API key.
        """
        while True:
            now = time.time()  # Get current time in seconds
            # Check each key in order to find the first available
            for i in range(len(self.keys)):
                key = self.keys[i]
                if now - self.cooldowns.get(key, 0) >= self.cooldown_time:
                    # Rotate the deque so the next key is checked first next time
                    self.keys.rotate(-i-1)
                    return key  # Return the first available key
            
            # If no keys are available, calculate the earliest time one will be available
            earliest_time = min(
                (self.cooldowns.get(k, 0) + self.cooldown_time for k in self.keys),
                default=None
            )
            if earliest_time:
                # Calculate how long to wait until the next key is available
                sleep_time = max(earliest_time - now, 0)
                logging.info(f"All keys on cooldown. Waiting {sleep_time:.1f} seconds...")
                await asyncio.sleep(sleep_time)  # Wait until one key becomes available
            else:
                # This case should never occur, but just in case, wait the full cooldown period
                logging.info("No keys available. Waiting...")
                await asyncio.sleep(self.cooldown_time)

    def mark_rate_limit(self, key):
        """
        Marks an API key as rate-limited by recording the time it was rate-limited.

        Args:
            key (str): The API key that has been rate-limited.
        """
        self.cooldowns[key] = time.time()  # Set the cooldown time for the given key
        logging.info(f"Key {key[-8:]} rate limited")  # Log the rate limiting event
