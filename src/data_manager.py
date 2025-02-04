import os
import csv
import re
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import box
import tqdm
import logging

logger = logging.getLogger(__name__)

class DataManager:
    """
    A class for managing data related to API keys, POI types, and bounding box operations.
    Handles file loading, logging, and progress tracking.
    """
    def __init__(self, config):
        """
        Initializes the DataManager with configurations and loads necessary data.

        Args:
            config (dict): Configuration dictionary containing file paths.
        """
        self.config = config
        self.api_keys = self._load_api_keys()
        self.poi_types = self._load_poi_types()
        self.initial_bbox = self._load_initial_bbox()
        self.api_requests_count = {key: 0 for key in self.api_keys}
        self._init_output_files()

        # Progress bar for tracking POI processing progress
        self.progress_bar = None    
    
    def _load_api_keys(self):
        """
        Loads API keys from a file.

        Returns:
            list[str]: List of API keys.

        Logs an error if the file is not found.
        """
        path = self.config['paths']['api_keys']
        try:
            with open(path) as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error(f"API keys file not found: {path}")
            return []

    def _load_poi_types(self):
        """
        Loads POI (Point of Interest) types from a file.

        Returns:
            list[str]: List of POI types.

        Logs an error if the file is not found.
        """
        path = self.config['paths']['poi_types']
        try:
            with open(path) as f:
                return [line.strip() for line in f if line.strip()]
        except FileNotFoundError:
            logger.error(f"POI types file not found: {path}")
            return []

    def _load_initial_bbox(self):
        """
        Loads the initial bounding box from a GeoJSON file.

        Returns:
            shapely.geometry.Polygon: Bounding box of the area of interest.

        Raises:
            Exception: If an error occurs while loading the GeoJSON file.
        """
        path = self.config['paths']['city_geojson']
        try:
            city = gpd.read_file(path)

            # Creates a bounding box around the city's geometry
            return box(*unary_union(city.geometry).bounds)   
        
        except Exception as e:
            logger.error(f"Error loading GeoJSON: {e}")
            raise

    def _init_output_files(self):
        """
        Initializes the necessary output files:
        - Output CSV for storing results
        - Queue log for tracking processed bounding boxes and POI types

        If files don't exist, they are created with appropriate headers.
        """
        # Initialize output CSV
        output_csv = self.config['paths']['output_csv']
        if not os.path.exists(output_csv):
            os.makedirs(os.path.dirname(output_csv), exist_ok=True)
            with open(output_csv, 'w', newline='', encoding='utf-8') as f:
                # Writing header row
                csv.writer(f).writerow(['place_id', 'Type'])

        # Initialize queue log
        queue_log = self.config['paths']['queue_log']
        if not os.path.exists(queue_log):
            os.makedirs(os.path.dirname(queue_log), exist_ok=True)
            # Create an empty file if it doesn't exist
            open(queue_log, 'a').close()

    def sanitize_poi_type_name(self, name):
        """
        Sanitizes POI type names by replacing special characters with underscores.

        Args:
            name (str): The POI type name.

        Returns:
            str: Sanitized POI type name.
        """
        return re.sub(r'[<>:"/\\|?*]', '_', name).strip()

    def log_queue_task(self, bbox, poi_type):
        """
        Logs a queue task entry to track processed bounding boxes and POI types.

        Args:
            bbox (shapely.geometry.Polygon): The bounding box being processed.
            poi_type (str): The POI type being queried.
        """
        path = self.config['paths']['queue_log']

        # Ensure POI type name is filesystem-safe
        safe_poi_type = self.sanitize_poi_type_name(poi_type)
        
        with open(path, 'a') as f:
            # Log the POI type and bounding box bounds
            f.write(f"{safe_poi_type}|{bbox.bounds}\n")

    async def save_results(self, results):
        """
        Saves API query results to the output CSV file.

        Args:
            results (list[list]): A list of lists, where each sublist represents a row of data.

        Returns:
            None
        """
        if not results:
            return
        output_path = self.config['paths']['output_csv']
        with open(output_path, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            
            # Append results to the CSV file
            writer.writerows(results)

    def initialize_progress_bar(self):
        """
        Initializes a progress bar to track the number of POI types processed.
        """
        self.progress_bar = tqdm.tqdm(total=len(self.poi_types), desc="POI Types Completed")

    def update_progress(self):
        """
        Updates the progress bar by one step.
        """
        if self.progress_bar:
            self.progress_bar.update(1)

    def close_progress_bar(self):
        """
        Closes the progress bar after completion.
        """
        if self.progress_bar:
            self.progress_bar.close()