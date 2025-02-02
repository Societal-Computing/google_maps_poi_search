import csv
import os
import re
import json
import geopandas as gpd
from shapely.ops import unary_union
from tqdm import tqdm
from shapely.geometry import box
from pathlib import Path
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
        with open(self.config['paths']['output_csv'], 'w', newline='', encoding='utf-8') as f:
            csv.writer(f).writerow(["place_id", "Type"])

    def save_api_requests(self, requests_count):
        try:
            with open(self.config['paths']['api_requests_json'], 'w', encoding='utf-8') as f:
                json.dump(requests_count, f, indent=4)
        except Exception as e:
            print(f"Could not save API request counts: {e}")

    def log_queue_task(self, bbox, poi_type):
        safe_poi_type = self.sanitize_filename(poi_type)
        with open(self.config['paths']['queue_log'], 'a') as f:
            f.write(f"{safe_poi_type}|{bbox.bounds}\n")

    def create_progress_bar(self, total):
        self.progress_bar = tqdm(total=total, desc="POI Types Completed")

    def update_progress(self):
        if self.progress_bar:
            self.progress_bar.update(1)