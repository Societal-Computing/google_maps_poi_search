import asyncio
import yaml
import logging
from .data_manager import DataManager
from .api_key_manager import APIKeyManager
from .bbox_utils import BBoxUtils
from .poi_searcher import POISearcher
from pathlib import Path
import time

def load_config(config_path = Path(__file__).parent.parent / 'data' / 'config.yaml'):
    """
    Loads the configuration file in YAML format.

    Args:
        config_path (Path): Path to the YAML configuration file.
    
    Returns:
        dict: Loaded configuration data as a dictionary.
    """
    with open(config_path) as f:
        return yaml.safe_load(f)

def main():
    """
    Main function that sets up the environment, initializes necessary components,
    and runs the POI search process. It also logs the execution time for performance tracking.
    """

    # Set up logging to output info level messages to the console
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )

    # Load the configuration file
    config = load_config()
    
    # Initialize the DataManager with the loaded configuration
    data_manager = DataManager(config)

    # Initialize the APIKeyManager to handle API keys and their usage limits
    api_key_manager = APIKeyManager(
        config=config,
        api_keys=data_manager.api_keys,
        cooldown_time=config['api']['cooldown_time'],
        api_requests_count=data_manager.api_requests_count
    )

    # Initialize the BBoxUtils class to handle bounding box-related utilities
    bbox_utils = BBoxUtils()

    # Initialize the POISearcher to handle Points of Interest (POI) searching
    poi_searcher = POISearcher(
        data_manager=data_manager,
        api_key_manager=api_key_manager,
        bbox_utils=bbox_utils,
        config=config
    )
    
    # Record the start time of the POI search process
    start_time = time.time()

    # Run the POI search process asynchronously
    asyncio.run(poi_searcher.run())

    # Record the end time and calculate the total execution time
    end_time = time.time()
    total_time = round(end_time - start_time, 2)

    # Log the total execution time
    logging.info(f"Total execution time: {total_time} seconds")

# Ensure the main function is executed when this script is run directly
if __name__ == "__main__":
    main()