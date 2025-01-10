import asyncio
import itertools
import json

class APIManager:
    """
    Manages API keys and usage for the Google Places API.
    """

    def __init__(self, api_keys_file, logger):
        """
        Initialize the APIManager.

        Args:
            api_keys_file (str): Path to the file containing API keys.
            logger: Logger object for logging messages.
        """
        self.logger = logger
        self.api_keys = self._load_api_keys(api_keys_file)
        self.api_request_counts = {key: 0 for key in self.api_keys}
        self.api_key_iterator = itertools.cycle(self.api_keys)
        self.api_lock = asyncio.Lock()

    def _load_api_keys(self, file_path):
        """
        Load API keys from a file.

        Args:
            file_path (str): Path to the file containing API keys.

        Returns:
            list: List of API keys.

        Raises:
            Exception: If there's an error loading the API keys.
        """
        api_keys = []
        try:
            with open(file_path, "r", encoding="utf-8") as file:
                for line in file:
                    api_key = line.split('#')[0].strip()
                    if api_key:
                        api_keys.append(api_key)
            self.logger.info(f"Loaded {len(api_keys)} API keys.")
        except Exception as e:
            self.logger.error(f"Error loading API keys: {e}")
            raise
        return api_keys

    async def get_next_api_key(self):
        """
        Get the next API key in the rotation.

        Returns:
            str: The next API key.
        """
        async with self.api_lock:
            return next(self.api_key_iterator)

    async def increment_api_request_count(self, api_key):
        """
        Increment the request count for a specific API key.

        Args:
            api_key (str): The API key to increment the count for.
        """
        async with self.api_lock:
            self.api_request_counts[api_key] += 1

    async def save_api_usage(self, output_file):
        """
        Save the API usage data to a JSON file.

        Args:
            output_file (str): Path to the output JSON file.
        """
        async with self.api_lock:
            try:
                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(self.api_request_counts, f, indent=4)
                self.logger.info(f"API usage data saved to {output_file}")
            except Exception as e:
                self.logger.error(f"Error saving API usage data: {e}")
