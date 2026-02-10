import asyncio
import yaml
import logging
from .data_manager import DataManager
from .api_key_manager import APIKeyManager
from .bbox_utils import BBoxUtils
from .poi_searcher import POISearcher
from pathlib import Path
import time
import cProfile
import pstats
import os
import argparse

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
        level=logging.DEBUG,
        format="[%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )

    # Parse command-line arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("--location", type=str, required=True, help="Location name (e.g., 'Berlin, Germany', 'Saarland', 'Central Park', 'Europe')")
    parser.add_argument("--place_types", type=str, required=True, help="Place types (e.g., 'supermarket,restaurant')")
    parser.add_argument("--skip-enrichment", action="store_true", help="Skip enrichment step; only write raw place_ids")
    args = parser.parse_args()

    # Load the configuration file
    config = load_config()
    
    # Create necessary directories
    for path_key in ['output_csv', 'queue_log', 'profiling_log', 'api_requests_json']:
        os.makedirs(os.path.dirname(config['paths'][path_key]), exist_ok=True)
    
    # Parse and clean place_types from comma-separated string
    place_types = [t.strip() for t in args.place_types.split(",") if t.strip()]
    print("DEBUG: place_types:", place_types)

    # Initialize the DataManager with the loaded configuration
    data_manager = DataManager(config, args.location, place_types)

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
    
    start_time = time.time()
    profiler = cProfile.Profile()
    profiler.enable()

    try:
        # Use asyncio.run with the new run_searcher function
        asyncio.run(poi_searcher.run())
    finally:
        profiler.disable()
        # Save profiling data
        profiling_log_path = config['paths']['profiling_log']
        with open(profiling_log_path, 'w') as f:
            ps = pstats.Stats(profiler, stream=f)
            ps.sort_stats(pstats.SortKey.CUMULATIVE)
            ps.print_stats()
            f.flush()
            os.fsync(f.fileno())

        # After search completes, optionally enrich the CSV with coordinates and JSON details
        try:
            if args.skip_enrichment or os.environ.get("POI_SKIP_ENRICHMENT", "0") == "1":
                logging.info("Skipping enrichment step")
            else:
                logging.info("Enriching results with coordinates...")
                data_manager.enrich_output_with_coordinates()
        except Exception as e:
            logging.error(f"Coordinate enrichment failed: {e}")

        end_time = time.time()
        total_time = round(end_time - start_time, 2)
        logging.info(f"Total execution time: {total_time} seconds")

# Ensure the main function is executed when this script is run directly
if __name__ == "__main__":
    main()