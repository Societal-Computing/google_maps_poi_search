import asyncio
import os
import time
import argparse
import nest_asyncio
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import box
from collections import deque
from src.config import Config
from src.logger import setup_logger
from src.api_manager import APIManager
from src.places_scraper import PlacesScraper

def parse_args():
    """
    Parse command-line arguments for the Google Places API Scraper.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Google Places API Scraper')
    parser.add_argument('--base-dir', required=True, help='Base directory for the project')
    parser.add_argument('--geojson-file', required=True, help='Path to input GeoJSON file')
    parser.add_argument('--threshold', type=int, help='Threshold for POI results')
    parser.add_argument('--max-concurrent', type=int, help='Maximum concurrent requests')
    parser.add_argument('--log-level', choices=['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'], default='DEBUG', help='Set the logging level')
    return parser.parse_args()

async def main():
    """
    Main asynchronous function to run the Google Places API Scraper.
    """
    # Parse command-line arguments
    args = parse_args()

    # Initialize configuration
    config = Config(args.base_dir)
    if args.max_concurrent:
        config.MAX_CONCURRENT_REQUESTS = args.max_concurrent
    config.create_directories()
    
    # Setup logging
    logger = setup_logger(config.log_file, args.log_level)
    
    # Initialize API manager and scraper
    api_manager = APIManager(config.api_keys_file, logger)
    scraper = PlacesScraper(config, api_manager, logger)
    
    try:
        # Load and process city boundary
        city_gdf = gpd.read_file(args.geojson_file)
        city_polygon = unary_union(city_gdf['geometry'])
        rectangle = box(*city_polygon.bounds)
        
        # Start scraping process
        start_time = time.time()
        txt_files = deque([os.path.join(config.txt_folder, f) for f in os.listdir(config.txt_folder) if f.endswith('.txt')])
        await scraper.process_txt_files(txt_files, rectangle, args.threshold)
        await api_manager.save_api_usage(config.output_json_file)
        end_time = time.time()
        
        # Log execution summary
        logger.info(f"Total execution time: {end_time - start_time:.2f} seconds")
        logger.info(f"Maximum concurrent requests: {config.MAX_CONCURRENT_REQUESTS}")
        logger.info(f"Threshold: {args.threshold}")
    except Exception as e:
        logger.critical(f"Error: {e}")
        raise

if __name__ == "__main__":
    # Apply nest_asyncio to allow running async code in Jupyter notebooks
    nest_asyncio.apply()
    # Run the main asynchronous function
    asyncio.run(main())
