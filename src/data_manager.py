import csv
import os
import re
import json
import geopandas as gpd
from shapely.ops import unary_union
from tqdm import tqdm
from shapely.geometry import box
from pathlib import Path
import logging

class DataManager:
    """
    A class for managing data-related tasks, including loading files, 
    handling geojson data, and saving results to CSV and JSON. It also 
    provides utilities for logging and progress tracking.
    """

    def __init__(self, config):
        """
        Initialize the DataManager with configuration parameters.

        Args:
            config (dict): Configuration dictionary containing paths for 
                           input/output files and other settings.
        """
        self.config = config
        self.progress_bar = None
        
        # Create output directory if it doesn't exist
        output_dir = Path(config['paths']['output_csv']).parent
        output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def sanitize_poi_name(name):
        """
        Sanitize a poi name by replacing invalid characters with underscores.

        Args:
            name (str): The filename to be sanitized.

        Returns:
            str: The sanitized poi name.
        """
        return re.sub(r'[<>:"/\\|?*]', '_', name).strip()

    def load_api_keys(self, file_path):
        """
        Load a file line by line and return a list of API keys.

        Args:
            file_path (str): Path to the file to load.

        Returns:
            list: A list of API keys from the file.
        
        Raises:
            Exception: If the file is not found.
        """
        try:
            with open(file_path) as f:
                return [line for line in f if line.strip()]
        except FileNotFoundError:
            raise Exception(f"File not found: {file_path}")

    def load_geojson(self, file_path):
        """
        Load a GeoJSON file and return the bounding box of the union of all geometries.

        Args:
            file_path (str): Path to the GeoJSON file to load.

        Returns:
            shapely.geometry.box: A bounding box representing the union of all geometries.
        """
        city = gpd.read_file(file_path)
        return box(*unary_union(city.geometry).bounds)

    def initialize_output_csv(self):
        """
        Initialize the output CSV file by opening it for writing and writing the header row.
        """
        # Open the CSV file once and keep it open
        self.csv_file = open(self.config['paths']['output_csv'], 'w', newline='', encoding='utf-8')
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(["place_id", "Type"])  # Write header row
        self.csv_file.flush()
        os.fsync(self.csv_file.fileno())  # Force OS-level write

    def save_results(self, rows):
        """
        Save results to the output CSV file.

        Args:
            rows (list of tuples): Data to write to the CSV file.
        """
        if self.csv_file is None:
            # Reopen in append mode if initialized mid-process
            self.csv_file = open(self.config['paths']['output_csv'], 'a', newline='', encoding='utf-8')
            self.csv_writer = csv.writer(self.csv_file)
        
        self.csv_writer.writerows(rows)  # Write rows to CSV
        self.csv_file.flush()
        os.fsync(self.csv_file.fileno())  # Force OS-level write

    def save_api_requests(self, requests_count):
        """
        Save the API request count data to a JSON file.

        Args:
            requests_count (dict): A dictionary containing the API request counts.
        """
        output_path = Path(self.config['paths']['api_requests_json'])
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(requests_count, f, indent=4)  # Write data as formatted JSON
                f.flush()
                os.fsync(f.fileno())  # Force OS-level write
        except Exception as e:
            logging.error(f"Error saving API requests: {e}")

    def __del__(self):
        """
        Ensure the CSV file is properly closed when the DataManager object is destroyed.
        """
        if self.csv_file and not self.csv_file.closed:
            self.csv_file.close()

    def log_queue_task(self, bbox, poi_type):
        """
        Log the details of a POI (Point of Interest) task to a queue log.

        Args:
            bbox (shapely.geometry.box): The bounding box for the POI task.
            poi_type (str): The type of POI being processed.
        """
        safe_poi_type = self.sanitize_poi_name(poi_type)
        with open(self.config['paths']['queue_log'], 'a') as f:
            f.write(f"{safe_poi_type}|{bbox.bounds}\n")  # Write POI type and bounding box

    def create_progress_bar(self, total):
        """
        Initialize a progress bar to track task completion.

        Args:
            total (int): The total number of tasks to complete.
        """
        self.progress_bar = tqdm(total=total, desc="POI Types Completed")

    def update_progress(self):
        """
        Update the progress bar by one step.
        """
        if self.progress_bar:
            self.progress_bar.update(1)
