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
    def __init__(self, config):
        self.config = config
        self.progress_bar = None
        
        # Create output directory if it doesn't exist
        output_dir = Path(config['paths']['output_csv']).parent
        output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def sanitize_filename(name):
        return re.sub(r'[<>:"/\\|?*]', '_', name).strip()

    def load_file(self, file_path, comment_char='#'):
        try:
            with open(file_path) as f:
                return [line.split(comment_char)[0].strip() for line in f if line.strip()]
        except FileNotFoundError:
            raise Exception(f"File not found: {file_path}")

    def load_geojson(self, file_path):
        city = gpd.read_file(file_path)
        return box(*unary_union(city.geometry).bounds)

    def initialize_output_csv(self):
        # Open the CSV file once and keep it open
        self.csv_file = open(self.config['paths']['output_csv'], 'w', newline='', encoding='utf-8')
        self.csv_writer = csv.writer(self.csv_file)
        self.csv_writer.writerow(["place_id", "Type"])
        self.csv_file.flush()
        os.fsync(self.csv_file.fileno())  # Force OS-level write

    def save_results(self, rows):
        if self.csv_file is None:
            # Reopen in append mode if initialized mid-process
            self.csv_file = open(self.config['paths']['output_csv'], 'a', newline='', encoding='utf-8')
            self.csv_writer = csv.writer(self.csv_file)
        
        self.csv_writer.writerows(rows)
        self.csv_file.flush()
        os.fsync(self.csv_file.fileno())  # Force OS-level write

    def save_api_requests(self, requests_count):
        output_path = Path(self.config['paths']['api_requests_json'])
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(requests_count, f, indent=4)
                f.flush()
                os.fsync(f.fileno())  # Force OS-level write
        except Exception as e:
            logging.error(f"Error saving API requests: {e}")

    def __del__(self):
        # Ensure file is closed when DataManager is destroyed
        if self.csv_file and not self.csv_file.closed:
            self.csv_file.close()

    def log_queue_task(self, bbox, poi_type):
        safe_poi_type = self.sanitize_filename(poi_type)
        with open(self.config['paths']['queue_log'], 'a') as f:
            f.write(f"{safe_poi_type}|{bbox.bounds}\n")

    def create_progress_bar(self, total):
        self.progress_bar = tqdm(total=total, desc="POI Types Completed")

    def update_progress(self):
        if self.progress_bar:
            self.progress_bar.update(1)