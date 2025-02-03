import asyncio
import yaml
import logging
import time
from pathlib import Path
from src.api_key_manager import KeyManager
from src.data_manager import DataManager
from src.bbox_utils import BBoxUtils
from src.poi_searcher import POISearcher

def load_config():
    """
    Loads the configuration from a YAML file.

    Returns:
        dict: A dictionary containing the configuration settings.
        
    Raises:
        Exception: If the configuration file cannot be found or read.
    """
    config_path = Path(__file__).parent.parent / 'data' / 'config.yaml'
    with open(config_path) as f:
        return yaml.safe_load(f)

def setup_logging():
    """
    Sets up the logging configuration to log messages to the console.
    The log level is set to INFO, and the format is set to display the 
    log level and the message.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )

async def main():
    """
    The main function to execute the entire process. It sets up the 
    logging, loads configuration, initializes necessary components 
    like data manager, key manager, and POI searcher, and runs the 
    POI searcher to process the tasks.

    This function is executed asynchronously using asyncio.

    Logs the execution time and handles any exceptions during the process.
    """
    setup_logging()  # Set up logging for the application
    config = load_config()  # Load configuration from the YAML file
    
    # Initialize DataManager for managing data-related tasks
    data_manager = DataManager(config)
    data_manager.initialize_output_csv()  # Initialize the output CSV file for saving results
    
    # Load API keys from the specified file
    api_keys = data_manager.load_api_keys(config['paths']['api_keys'])
    if not api_keys:
        raise Exception("No API keys found")
    
    # Initialize KeyManager to manage API keys and handle rate limiting
    key_manager = KeyManager(
        api_keys=api_keys,
        cooldown_time=config['api']['cooldown_time']
    )
    
    # Initialize BBoxUtils for working with bounding boxes
    bbox_utils = BBoxUtils()
    
    start_time = time.time()  # Record the start time of the process

    # Initialize POISearcher for searching points of interest (POI)
    searcher = POISearcher(
        config=config,
        data_manager=data_manager,
        key_manager=key_manager,
        bbox_utils=bbox_utils
    )
    
    try:
        # Run the POI searcher to process the tasks asynchronously
        await searcher.run()
    except Exception as e:
        # Log any error that occurs during processing
        logging.error(f"Error during processing: {e}")
    finally:
        # Calculate and log the total execution time
        end_time = time.time()
        total_time = round(end_time - start_time, 2)
        logging.info(f"Total execution time: {total_time} seconds")

if __name__ == "__main__":
    """
    The entry point of the script. It starts the asynchronous process by
    calling the main function inside an asyncio event loop.
    """
    asyncio.run(main())
