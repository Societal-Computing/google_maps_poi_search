import asyncio
import csv
import os
from collections import deque
from shapely.geometry import box
from shapely.ops import unary_union
from geopy.distance import geodesic
import geopandas as gpd
import aiohttp

class PlacesScraper:
    """
    A class for scraping Points of Interest (POIs) using the Google Places API.
    """

    def __init__(self, config, api_manager, logger):
        """
        Initialize the PlacesScraper.

        Args:
            config (Config): Configuration object containing settings.
            api_manager (APIManager): Manager for API keys and usage.
            logger (Logger): Logger for recording events.
        """
        self.config = config
        self.api_manager = api_manager
        self.logger = logger
        self.saved_place_ids = set()
        self.completed_poi_types = set()
        self.semaphore = asyncio.Semaphore(config.MAX_CONCURRENT_REQUESTS)
        
        # Initialize CSV file if it doesn't exist
        if not os.path.exists(config.output_csv_file):
            with open(config.output_csv_file, "w", newline='', encoding="utf-8") as f:
                writer = csv.writer(f)
                writer.writerow(["place_id", "Type"])

    async def save_pois_to_csv(self, pois, poi_type):
        """
        Save new POIs to CSV file.

        Args:
            pois (list): List of POIs to save.
            poi_type (str): Type of POI.
        """
        new_pois = []
        for poi_id, poi_type in pois:
            if poi_id not in self.saved_place_ids:
                self.saved_place_ids.add(poi_id)
                new_pois.append((poi_id, poi_type))
        
        if new_pois:
            try:
                with open(self.config.output_csv_file, "a", newline='', encoding="utf-8") as f:
                    writer = csv.writer(f)
                    writer.writerows(new_pois)
                self.logger.info(f"Saved {len(new_pois)} new POIs for type '{poi_type}'.")
            except Exception as e:
                self.logger.error(f"Error writing to CSV: {e}")

    async def search_pois(self, rectangle, api_key, poi_type, data, retries=5, delay=2):
        """
        Search for POIs within a given rectangle using the Google Places API.

        Args:
            rectangle (shapely.geometry.box): Bounding box for the search area.
            api_key (str): Google Places API key.
            poi_type (str): Type of POI to search for.
            data (dict): Additional data for the API request.
            retries (int): Number of retries for failed requests.
            delay (int): Initial delay between retries in seconds.

        Returns:
            list: List of found POIs.
        """
        async with self.semaphore:
            url = "https://places.googleapis.com/v1/places:searchText"
            headers = {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": api_key,
                "X-Goog-FieldMask": "places.id"
            }
            min_lng, min_lat, max_lng, max_lat = rectangle.bounds
            data["locationRestriction"] = {
                "rectangle": {
                    "low": {"latitude": min_lat, "longitude": min_lng},
                    "high": {"latitude": max_lat, "longitude": max_lng}
                }
            }
            
            results = []
            for attempt in range(retries):
                try:
                    async with aiohttp.ClientSession() as session:
                        async with session.post(url, headers=headers, json=data) as response:
                            if response.status == 200:
                                await self.api_manager.increment_api_request_count(api_key)
                                response_data = await response.json()
                                for place in response_data.get("places", []):
                                    results.append((place["id"], poi_type))
                                self.logger.info(f"API key {api_key} used for POI type '{poi_type}'")
                                return results
                            elif response.status == 429:
                                api_key = await self.api_manager.get_next_api_key()
                            else:
                                self.logger.error(f"Unexpected error: {await response.text()}")
                                break
                except Exception as e:
                    self.logger.error(f"Error during request: {e}")
                await asyncio.sleep(delay)
                delay = min(delay * 2, 60)
            return results

    def get_longest_side(self, bounding_box):
        """
        Calculate the longest side of a bounding box in meters.

        Args:
            bounding_box (shapely.geometry.box): The bounding box to measure.

        Returns:
            float: Length of the longest side in meters.
        """
        min_lng, min_lat, max_lng, max_lat = bounding_box.bounds
        bottom_left = (min_lat, min_lng)
        bottom_right = (min_lat, max_lng)
        top_left = (max_lat, min_lng)
        width = geodesic(bottom_left, bottom_right).meters
        height = geodesic(bottom_left, top_left).meters
        return max(width, height)

    def dynamic_overlap(self, bounding_box):
        """
        Determine the overlap for quadtree division based on bounding box size.

        Args:
            bounding_box (shapely.geometry.box): The bounding box to analyze.

        Returns:
            float: Overlap value in degrees.
        """
        longest_side = self.get_longest_side(bounding_box)
        if longest_side > 1000:
            return 0.0018  # 200 meters
        if longest_side > 500:
            return 0.0009  # 100 meters
        elif longest_side > 200:
            return 0.0005  # 55 meters
        elif longest_side > 100:
            return 0.0003  # 33 meters
        elif longest_side > 50:
            return 0.0001  # 11 meters
        return 0.0

    def divide_box(self, boundary_box, depth, quadrant_number, use_overlap, bounding_box):
        """
        Divide a bounding box into four quadrants with optional overlap.

        Args:
            boundary_box (shapely.geometry.box): The box to divide.
            depth (int): Current depth of quadtree.
            quadrant_number (int): Current quadrant number.
            use_overlap (bool): Whether to use overlap in division.
            bounding_box (shapely.geometry.box): Original bounding box for overlap calculation.

        Returns:
            list: Four shapely.geometry.box objects representing the quadrants.
        """
        min_lng, min_lat, max_lng, max_lat = boundary_box.bounds
        mid_lng, mid_lat = (min_lng + max_lng) / 2, (min_lat + max_lat) / 2

        overlap = self.dynamic_overlap(bounding_box) if use_overlap else 0

        return [
            box(min_lng, min_lat, mid_lng + overlap, mid_lat + overlap),  # Bottom-left
            box(mid_lng - overlap, min_lat, max_lng, mid_lat + overlap),  # Bottom-right
            box(min_lng, mid_lat - overlap, mid_lng + overlap, max_lat),  # Top-left
            box(mid_lng - overlap, mid_lat - overlap, max_lng, max_lat)   # Top-right
        ]

    def normalize_coords(self, coords, precision=5):
        """
        Normalize coordinates to a specified precision.

        Args:
            coords (tuple): Coordinates to normalize.
            precision (int): Number of decimal places to round to.

        Returns:
            tuple: Normalized coordinates.
        """
        return tuple(round(c, precision) for c in coords)

    async def quadtree_search(self, bounding_box, api_key, poi_type, data, threshold, quadrant_number=1, searched_areas=None, depth=0):
        """
        Perform a quadtree search for POIs within a bounding box.

        Args:
            bounding_box (shapely.geometry.box): Area to search.
            api_key (str): Google Places API key.
            poi_type (str): Type of POI to search for.
            data (dict): Additional search parameters.
            threshold (int): Threshold for subdividing the search area.
            quadrant_number (int): Current quadrant number.
            searched_areas (set): Set of already searched areas.
            depth (int): Current depth of quadtree.

        Returns:
            set: Set of POIs found.
        """
        if searched_areas is None:
            searched_areas = set()

        results = set()
        use_overlap = True

        bounding_box_coords = self.normalize_coords(bounding_box.bounds)
        if bounding_box_coords in searched_areas:
            self.logger.info(f"Bounding box {bounding_box_coords} has already been searched. Skipping...")
            return results

        searched_areas.add(bounding_box_coords)
        self.logger.info(f"\nSearching depth {depth}, quadrant {quadrant_number}, bounding box {bounding_box_coords}")

        pois = await self.search_pois(bounding_box, api_key, poi_type, data)
        self.logger.info(f"Number of places found: {len(pois)}")

        await self.save_pois_to_csv(pois, poi_type)
        results.update(pois)

        if self.get_longest_side(bounding_box) < 10:
            self.logger.info(f"Skipping division for bounding box {bounding_box_coords} with side length < 10 meters.")
            return results

        if len(pois) > threshold:
            quadrants = self.divide_box(bounding_box, depth, quadrant_number, use_overlap, bounding_box)
            self.logger.info("*" * 50)

            for i, quadrant in enumerate(quadrants):
                self.logger.info(f"Searching depth {depth+1} at quadrant {i+1}")
                quadrant_results = await self.quadtree_search(
                    quadrant, api_key, poi_type, data, threshold, i+1, searched_areas, depth+1
                )
                results.update(quadrant_results)
        else:
            self.logger.info(f"Number of POIs {len(pois)} is below threshold {threshold}. Continuing search.")
        
        return results

    def append_completed_poi_type(self, poi_type):
        """
        Append a completed POI type to the file of completed searches.

        Args:
            poi_type (str): The POI type that has been completed.
        """
        if poi_type in self.completed_poi_types:
            return
        self.completed_poi_types.add(poi_type)
        try:
            with open(self.config.completed_poi_types_file, "a", encoding="utf-8") as file:
                file.write(f"{poi_type}\n")
            self.logger.info(f"POI type '{poi_type}' saved as completed.")
        except Exception as e:
            self.logger.error(f"Error saving POI type '{poi_type}': {e}")

    async def search_pois_for_type(self, poi_type, rectangle, threshold):
        """
        Search for POIs of a specific type within a given area.

        Args:
            poi_type (str): Type of POI to search for.
            rectangle (shapely.geometry.box): Area to search within.
            threshold (int): Threshold for subdividing the search area.

        Returns:
            list: List of POIs found.
        """
        self.logger.info(f"Searching POI type '{poi_type}'")
        data = {"textQuery": poi_type}
        if "restaurant" in poi_type.lower():
            data["includedType"] = "restaurant"
        
        api_key = await self.api_manager.get_next_api_key()
        pois = await self.quadtree_search(rectangle, api_key, poi_type, data, threshold)
        self.append_completed_poi_type(poi_type)
        return pois

    async def process_txt_files(self, txt_files, rectangle, threshold):
        """
        Process text files containing POI types and search for POIs.

        Args:
            txt_files (deque): Queue of text files to process.
            rectangle (shapely.geometry.box): Bounding box for the search area.
            threshold (int): Threshold for POI results.
        """
        tasks = []

        while txt_files:
            file_path = txt_files.popleft()
            with open(file_path, "r", encoding="utf-8") as f:
                poi_types = [line.strip() for line in f]
            
            for poi_type in poi_types:
                tasks.append(self.search_pois_for_type(poi_type, rectangle, threshold))

            if tasks:
                await asyncio.gather(*tasks)
                tasks.clear()