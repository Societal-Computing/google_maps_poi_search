import os
import csv
import re
import geopandas as gpd
from shapely.ops import unary_union
from shapely.geometry import box, MultiPolygon
import tqdm
import asyncio
import logging
import osmnx as ox
from typing import Dict, Tuple
import glob
from pathlib import Path

logger = logging.getLogger(__name__)

class DataManager:
    """
    A class for managing data related to API keys, POI types, and bounding box operations.
    Handles file loading, logging, and progress tracking.
    """
    def __init__(self, config, location_name=None, place_types=None):
        """
        Initializes the DataManager with configurations and loads necessary data.

        Args:
            config (dict): Configuration dictionary containing file paths.
        """
        self.config = config
        # Allow overriding output CSV path per run via env var to avoid cross-location overwrite
        self.output_csv = os.environ.get('POI_OUTPUT_CSV_PATH', self.config['paths']['output_csv'])
        self.api_keys = self._load_api_keys()
        # self.poi_types = self._load_poi_types()
        self.poi_types = place_types
        self.location_name = location_name
        # Ensure output CSV exists with header even if no results are found
        try:
            os.makedirs(os.path.dirname(self.output_csv), exist_ok=True)
            if not os.path.exists(self.output_csv) or os.path.getsize(self.output_csv) == 0:
                with open(self.output_csv, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(["place_id", "place_type", "location", "geometry_class", "geometry_type", "address_type", "admin_level"])
        except Exception:
            pass
        # Extract OSM details for the location
        try:
            gdf = ox.geocode_to_gdf(location_name)
            gdf['geometry'] = gdf.geometry.apply(self._extract_main_geometry)
        except Exception as e:
            logger.warning(f"geocode_to_gdf failed for '{location_name}' ({e}); falling back to point-based bbox")
            try:
                geom = ox.geocode(location_name)
            except Exception as e2:
                logger.error(f"Fallback geocode failed for '{location_name}': {e2}")
                geom = None
            if geom is not None and hasattr(geom, 'bounds'):
                # If a point geometry, build a small bbox around it (approx ~1km square depending on latitude)
                try:
                    minx, miny, maxx, maxy = geom.bounds
                    if minx == maxx and miny == maxy:
                        # Treat as point
                        d = 0.01
                        bbox_poly = box(geom.x - d, geom.y - d, geom.x + d, geom.y + d)
                    else:
                        bbox_poly = box(*geom.bounds)
                    gdf = gpd.GeoDataFrame({'geometry': [bbox_poly]}, crs='EPSG:4326')
                except Exception as e3:
                    logger.error(f"Error creating fallback bbox: {e3}")
                    gdf = gpd.GeoDataFrame({'geometry': []}, crs='EPSG:4326')
            else:
                gdf = gpd.GeoDataFrame({'geometry': []}, crs='EPSG:4326')





        # Persist the resolved geometry for inspection/debugging if a path is provided
        geojson_path = self.config['paths'].get('location_geojson')
        if geojson_path and location_name:
            try:
                safe_location = self.sanitize_poi_type_name(location_name).replace(' ', '_')
                base_path = Path(geojson_path)
                if base_path.suffix.lower() == '.geojson':
                    target_path = base_path.with_name(f"{safe_location}.geojson")
                    target_dir = target_path.parent
                else:
                    target_dir = base_path
                    target_path = base_path / f"{safe_location}.geojson"
                target_dir.mkdir(parents=True, exist_ok=True)
                gdf.to_file(target_path, driver='GeoJSON')
                logger.info(f"Saved GeoJSON to path {target_path}")
            except Exception as save_err:
                logger.warning(f"Could not save GeoJSON for '{location_name}': {save_err}")






        gdf = gdf.explode(index_parts=False).reset_index(drop=True)
        gdf['geometry'] = gdf.geometry.make_valid()
        if not gdf.empty:
            row = gdf.iloc[0]
            # Get all admin boundaries in the area and take the most specific one
            try:
                admin_gdf = ox.features.features_from_place(location_name, {"boundary": "administrative"})
                admin_gdf = admin_gdf[admin_gdf.geometry.notnull()]
                if not admin_gdf.empty:
                    # Extract the main location name (first part before comma)
                    main_location = location_name.split(',')[0].strip()
                    admin_level = ""
                    
                    # First, try to find a boundary that matches the main location name
                    for idx, boundary in admin_gdf.iterrows():
                        boundary_name = boundary.get('name', '')
                        if boundary_name and main_location.lower() in boundary_name.lower():
                            admin_level = boundary.get("admin_level", "")
                            logger.info(f"Found matching boundary '{boundary_name}' with admin_level {admin_level} for main location '{main_location}'")
                            break
                    
                    # If no name match found, try exact match
                    if not admin_level:
                        for idx, boundary in admin_gdf.iterrows():
                            boundary_name = boundary.get('name', '')
                            if boundary_name and boundary_name.lower() == main_location.lower():
                                admin_level = boundary.get("admin_level", "")
                                logger.info(f"Found exact boundary match '{boundary_name}' with admin_level {admin_level}")
                                break
                    
                    # If still no match, fall back to smallest area (most specific to the location)
                    if not admin_level:
                        idx = admin_gdf.area.idxmin()
                        admin_row = admin_gdf.loc[idx]
                        admin_level = admin_row.get("admin_level", "")
                        boundary_name = admin_row.get('name', 'Unknown')
                        logger.warning(f"No name match found for '{main_location}', using smallest area boundary '{boundary_name}' with admin_level {admin_level}")
                else:
                    admin_level = ""
            except Exception as e:
                logger.warning(f"Could not retrieve admin_level for {location_name}: {e}")
                admin_level = ""
            self.location_osm_details = {
                'geometry_class': row.get('class', ''),
                'geometry_type': row.get('type', ''),
                'address_type': row.get('addresstype', ''),
                'admin_level': admin_level
            }
        else:
            self.location_osm_details = {'geometry_class': '', 'geometry_type': '', 'address_type': '', 'admin_level': ''}
        self.initial_bboxes = [box(*geom.bounds) for geom in gdf.geometry]
        try:
            logger.info("Initialized %d bounding box(es) after main-geometry filtering", len(self.initial_bboxes))
        except Exception:
            pass
        self.api_requests_count = {key: 0 for key in self.api_keys}

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

    # def _load_initial_bbox(self):
    #     """
    #     Loads the initial bounding box from a GeoJSON file.

    #     Returns:
    #         shapely.geometry.Polygon: Bounding box of the area of interest.

    #     Raises:
    #         Exception: If an error occurs while loading the GeoJSON file.
    #     """
    #     path = self.config['paths']['city_geojson']
    #     try:
    #         city = gpd.read_file(path)
    #         # # Creates a bounding box around the city's geometry
    #         # return box(*unary_union(city.geometry).bounds) 
            
    #         # Explode MultiPolygons into individual Polygons
    #         city_exploded = city.explode(index_parts=False).reset_index(drop=True)
            
    #         # Ensure all geometries are valid
    #         city_exploded['geometry'] = city_exploded['geometry'].make_valid()
            
    #         # Convert each polygon to a bounding box
    #         bboxes = [box(*geom.bounds) for geom in city_exploded.geometry]
    #         return bboxes
            
    #     except Exception as e:
    #         logger.error(f"Error loading GeoJSON: {e}")
    #         raise

    def _load_initial_bbox(self, location_name):
        """Generate bounding boxes from location name using osmnx"""
        try:
            gdf = ox.geocode_to_gdf(location_name)
            gdf['geometry'] = gdf.geometry.apply(self._extract_main_geometry)
            gdf = gdf.explode(index_parts=False).reset_index(drop=True)
            gdf['geometry'] = gdf.geometry.make_valid()
            return [box(*geom.bounds) for geom in gdf.geometry]
        except Exception as e:
            logger.error(f"Error generating GeoJSON: {e}")
            raise

    @staticmethod
    def _extract_main_geometry(geom):
        """
        Return the largest polygon from a (Multi)Polygon to focus on the main landmass.
        This helps avoid tiny offshore islands when generating bounding boxes.
        """
        if isinstance(geom, MultiPolygon):
            return max(geom.geoms, key=lambda g: g.area)
        return geom

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
        output_path = self.output_csv
        try:
            async with asyncio.Lock():
                file_exists = os.path.exists(output_path)
                write_header = not file_exists or os.path.getsize(output_path) == 0
                with open(output_path, "a", newline="", encoding="utf-8") as f:
                    writer = csv.writer(f)
                    osm_keys = ['geometry_class', 'geometry_type', 'address_type', 'admin_level']
                    if write_header:
                        writer.writerow(["place_id", "place_type", "location"] + osm_keys)
                    for row in results:
                        writer.writerow(list(row) + [self.location_name] + [self.location_osm_details.get(k, "") for k in osm_keys])
                    f.flush()
                    if hasattr(os, 'fsync'):
                        os.fsync(f.fileno())
                    logger.debug(f"Saved {len(results)} POIs to {output_path}")
        except Exception as e:
            logger.error(f"Error saving results to CSV: {e}")
            raise
        

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

    def enrich_output_with_coordinates(self) -> None:
        """
        Read the existing output CSV, use LiveBusynessExtractor to fetch coordinates per place_id,
        and rewrite with latitude/longitude columns appended. After enrichment, filter rows to
        include only those whose coordinates lie within the location polygon and whose JSON type
        matches one of the requested poi_types (json_data[6][13][0]).
        Requires PROXY_USERNAME and PROXY_PASSWORD env vars and proxies file.
        """
        try:
            from pathlib import Path
            import sys
            root_dir = Path(__file__).resolve().parents[2]
            extractor_dir = root_dir / 'live_busyness_extraction'
            if str(extractor_dir) not in sys.path:
                sys.path.append(str(extractor_dir))
            from extractor import LiveBusynessExtractor  # type: ignore
        except Exception as e:
            logger.error(f"Cannot import LiveBusynessExtractor: {e}")
            return

        output_path = self.output_csv
        if not os.path.exists(output_path) or os.path.getsize(output_path) == 0:
            logger.warning("Output CSV not found or empty; skipping coordinate enrichment")
            return

        try:
            # Read existing CSV
            with open(output_path, 'r', newline='', encoding='utf-8') as f:
                reader = list(csv.reader(f))
            if not reader:
                return
            header = reader[0]
            rows = reader[1:]

            # Detect place_id column
            if 'place_id' in header:
                place_id_col = header.index('place_id')
            elif 'id' in header:
                place_id_col = header.index('id')
            else:
                place_id_col = 0

            # Detect if coordinates already exist; we will STILL run filtering and can refresh coords
            coords_in_header = ('latitude' in header and 'longitude' in header)
            lat_idx = header.index('latitude') if 'latitude' in header else None
            lng_idx = header.index('longitude') if 'longitude' in header else None

            # Use direct credentials (requested)
            proxy_user = 'iweber02'
            proxy_pass = 'qp9dQbDM'
            # Prefer latest working proxies generated by live_busyness_extraction
            working_proxies_dir = root_dir / 'live_busyness_extraction' / 'input' / 'working_proxies'
            proxies_file = None
            try:
                candidates = glob.glob(str(working_proxies_dir / 'working_proxies_*.txt'))
                if candidates:
                    proxies_file = max(candidates, key=os.path.getctime)
            except Exception:
                proxies_file = None
            # Fallback to api_json_extraction/data/proxies.txt
            if not proxies_file:
                p2 = root_dir / 'api_json_extraction' / 'data' / 'proxies.txt'
                proxies_file = str(p2) if p2.exists() else None

            if not proxy_user or not proxy_pass or not proxies_file or not os.path.exists(proxies_file):
                logger.warning("Missing proxy credentials or proxies file; skipping coordinate enrichment")
                return

            extractor = LiveBusynessExtractor(proxy_user, proxy_pass)
            extractor.load_proxies(proxies_file)

            # Build location polygon union (with fallback if polygon not available)
            try:
                poly_gdf = ox.geocode_to_gdf(self.location_name)
                poly_gdf['geometry'] = poly_gdf.geometry.apply(self._extract_main_geometry)
            except Exception as e:
                logger.warning(f"Polygon fetch failed for '{self.location_name}' ({e}); falling back to point/bbox")
                try:
                    geom = ox.geocode(self.location_name)
                except Exception as e2:
                    logger.error(f"Fallback geocode failed for '{self.location_name}': {e2}")
                    geom = None
                if geom is not None and hasattr(geom, 'bounds'):
                    try:
                        minx, miny, maxx, maxy = geom.bounds
                        if minx == maxx and miny == maxy:
                            d = 0.01
                            bbox_poly = box(geom.x - d, geom.y - d, geom.x + d, geom.y + d)
                        else:
                            bbox_poly = box(*geom.bounds)
                        poly_gdf = gpd.GeoDataFrame({'geometry': [bbox_poly]}, crs='EPSG:4326')
                    except Exception as e3:
                        logger.error(f"Error creating fallback bbox: {e3}")
                        poly_gdf = gpd.GeoDataFrame({'geometry': []}, crs='EPSG:4326')
                else:
                    poly_gdf = gpd.GeoDataFrame({'geometry': []}, crs='EPSG:4326')

            if not poly_gdf.empty:
                poly_gdf = poly_gdf.explode(index_parts=False).reset_index(drop=True)
                poly_gdf['geometry'] = poly_gdf['geometry'].make_valid()
                try:
                    poly_union = poly_gdf.unary_union
                except Exception:
                    poly_union = unary_union(poly_gdf.geometry)
            else:
                poly_union = None

            # Fetch coords and json types for unique ids using extractor (parallelized per proxy)
            unique_ids = list({r[place_id_col] for r in rows if len(r) > place_id_col and r[place_id_col]})
            id_to_coords: Dict[str, Tuple[float, float]] = {}
            # Store full list of json types found at json_data[6][13]
            id_to_json_types: Dict[str, list] = {}
            # Store json_url and opening hours per id
            id_to_json_url: Dict[str, str] = {}
            id_to_opening_hours: Dict[str, str] = {}
            kept_count = 0
            type_miss = 0
            polygon_miss = 0
            total_fetched = 0

            try:
                import threading
                from concurrent.futures import ThreadPoolExecutor, as_completed
                import random
                import time as _time

                proxies = list(extractor.proxies) if hasattr(extractor, 'proxies') else []
                if not proxies:
                    logger.warning("No proxies loaded; falling back to sequential processing")
                    proxies = [extractor.get_next_proxy(None)] if hasattr(extractor, 'get_next_proxy') else []

                # Assign place_ids to proxies in round-robin
                assignment = {px: [] for px in proxies}
                for idx, pid in enumerate(unique_ids):
                    px = proxies[idx % len(proxies)]
                    assignment[px].append(pid)

                lock = threading.Lock()

                def _worker(px: str, pids: list):
                    nonlocal total_fetched
                    local_count = 0
                    for pid in pids:
                        t0 = _time.perf_counter()
                        success = False
                        blocked = False
                        
                        try:
                            # Try multiple proxies for this place_id (start with assigned px)
                            proxy_order = [px] + [p for p in proxies if p != px]
                            for attempt in range(3):  # max_retries per PID
                                for use_px in proxy_order:
                                    try:
                                        json_url = extractor.get_json_url(pid, use_px)
                                        if not json_url:
                                            continue
                                        data = extractor.get_json_data(json_url, use_px, pid)
                                        if not data:
                                            continue
                                        coords = extractor.extract_coordinates(data)
                                        lat = coords.get('lat') if isinstance(coords, dict) else None
                                        lng = coords.get('lng') if isinstance(coords, dict) else None
                                        opening_hours_dict = extractor.extract_opening_hours(data) if hasattr(extractor, 'extract_opening_hours') else None
                                        # Serialize opening hours as human-readable string to avoid double quotes in CSV
                                        try:
                                            if opening_hours_dict:
                                                # Prefer standard weekday order
                                                weekday_order = [
                                                    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
                                                ]
                                                parts = []
                                                for day in weekday_order:
                                                    hours = opening_hours_dict.get(day)
                                                    if hours:
                                                        if isinstance(hours, list):
                                                            hours_text = ", ".join(map(str, hours))
                                                        else:
                                                            hours_text = str(hours)
                                                        parts.append(f"{day}: {hours_text}")
                                                # Fallback for any extra keys not in the standard order
                                                for day, hours in opening_hours_dict.items():
                                                    if day not in weekday_order:
                                                        if isinstance(hours, list):
                                                            hours_text = ", ".join(map(str, hours))
                                                        else:
                                                            hours_text = str(hours)
                                                        parts.append(f"{day}: {hours_text}")
                                                opening_hours_str = "; ".join(parts) if parts else None
                                            else:
                                                opening_hours_str = None
                                        except Exception:
                                            opening_hours_str = None
                                        poi_json_types = None
                                        try:
                                            if isinstance(data, list) and len(data) > 6 and isinstance(data[6], list) and len(data[6]) > 13:
                                                t = data[6][13]
                                                if isinstance(t, list) and len(t) > 0:
                                                    poi_json_types = t
                                        except Exception:
                                            poi_json_types = None
                                        with lock:
                                            id_to_coords[pid] = (lat, lng)
                                            id_to_json_types[pid] = poi_json_types
                                            id_to_json_url[pid] = json_url
                                            id_to_opening_hours[pid] = opening_hours_str
                                            total_fetched += 1
                                            local_count += 1
                                        success = True
                                        break
                                    except Exception as e:
                                        logger.debug(f"Proxy {use_px} failed for {pid}: {e}")
                                        continue
                                if success:
                                    break
                            if not success:
                                blocked = True
                                with lock:
                                    id_to_coords[pid] = (None, None)
                                    id_to_json_types[pid] = None
                                    id_to_json_url[pid] = None
                                    id_to_opening_hours[pid] = None
                        except Exception as e:
                            logger.debug(f"Proxy worker error on {px} for {pid}: {e}")
                            blocked = True
                            with lock:
                                id_to_coords[pid] = (None, None)
                                id_to_json_types[pid] = None
                        
                        # Apply human-like delays and cooldown logic
                        if blocked and not success:
                            # Cooldown on proxy blocks (30-60 seconds)
                            cooldown = random.uniform(30, 60)
                            _time.sleep(cooldown)
                        else:
                            # Human-like delay between requests (0.01-0.2 seconds)
                            delay = random.uniform(0.01, 0.2)
                            _time.sleep(delay)
                    
                    return local_count

                # Run workers
                max_workers = max(1, len(proxies))
                with ThreadPoolExecutor(max_workers=max_workers) as executor:
                    futures = []
                    for px, pids in assignment.items():
                        if not pids:
                            continue
                        futures.append(executor.submit(_worker, px, pids))
                    done = 0
                    for fut in as_completed(futures):
                        try:
                            done += fut.result() or 0
                            if done % 50 == 0:
                                logger.info(f"Fetched coordinates for ~{done}/{len(unique_ids)} place_ids")
                        except Exception as e:
                            logger.error(f"Proxy worker raised: {e}")
            except Exception as e:
                logger.error(f"Parallel coordinate enrichment failed, continuing sequentially: {e}")
                for idx, pid in enumerate(unique_ids, 1):
                    try:
                        proxy = extractor.get_next_proxy(pid)
                        json_url = extractor.get_json_url(pid, proxy)
                        if not json_url:
                            id_to_coords[pid] = (None, None)
                            id_to_json_types[pid] = None
                            id_to_json_url[pid] = None
                            id_to_opening_hours[pid] = None
                            continue
                        data = extractor.get_json_data(json_url, proxy, pid)
                        if not data:
                            id_to_coords[pid] = (None, None)
                            id_to_json_types[pid] = None
                            id_to_json_url[pid] = None
                            id_to_opening_hours[pid] = None
                            continue
                        coords = extractor.extract_coordinates(data)
                        lat = coords.get('lat') if isinstance(coords, dict) else None
                        lng = coords.get('lng') if isinstance(coords, dict) else None
                        opening_hours_dict = extractor.extract_opening_hours(data) if hasattr(extractor, 'extract_opening_hours') else None
                        try:
                            if opening_hours_dict:
                                weekday_order = [
                                    "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"
                                ]
                                parts = []
                                for day in weekday_order:
                                    hours = opening_hours_dict.get(day)
                                    if hours:
                                        if isinstance(hours, list):
                                            hours_text = ", ".join(map(str, hours))
                                        else:
                                            hours_text = str(hours)
                                        parts.append(f"{day}: {hours_text}")
                                for day, hours in opening_hours_dict.items():
                                    if day not in weekday_order:
                                        if isinstance(hours, list):
                                            hours_text = ", ".join(map(str, hours))
                                        else:
                                            hours_text = str(hours)
                                        parts.append(f"{day}: {hours_text}")
                                opening_hours_str = "; ".join(parts) if parts else None
                            else:
                                opening_hours_str = None
                        except Exception:
                            opening_hours_str = None
                        poi_json_types = None
                        try:
                            if isinstance(data, list) and len(data) > 6 and isinstance(data[6], list) and len(data[6]) > 13:
                                t = data[6][13]
                                if isinstance(t, list) and len(t) > 0:
                                    poi_json_types = t
                        except Exception:
                            poi_json_types = None
                        id_to_coords[pid] = (lat, lng)
                        id_to_json_types[pid] = poi_json_types
                        id_to_json_url[pid] = json_url
                        id_to_opening_hours[pid] = opening_hours_str
                        total_fetched += 1
                    except Exception as e2:
                        logger.debug(f"Coordinate fetch failed for {pid}: {e2}")
                        id_to_coords[pid] = (None, None)
                        id_to_json_types[pid] = None
                    if idx % 50 == 0:
                        logger.info(f"Fetched coordinates for {idx}/{len(unique_ids)} place_ids")

            # Compose new CSV with appended columns, filtering by polygon and json type
            # Ensure columns for latitude/longitude/json_url/opening_hours exist
            new_header = list(header)
            if 'latitude' not in new_header:
                new_header.append('latitude')
            if 'longitude' not in new_header:
                new_header.append('longitude')
            if 'json_url' not in new_header:
                new_header.append('json_url')
            if 'opening_hours' not in new_header:
                new_header.append('opening_hours')
            new_rows = [new_header]
            # Control filtering via env vars (default: disabled)
            import os as _os
            filter_by_json_type = _os.environ.get('POI_FILTER_BY_JSON_TYPE', '0') == '1'
            filter_by_polygon = _os.environ.get('POI_FILTER_BY_POLYGON', '0') == '1'

            for r in rows:
                pid = r[place_id_col] if len(r) > place_id_col else ''
                lat, lng = id_to_coords.get(pid, (None, None))
                poi_json_types = id_to_json_types.get(pid)
                json_url_val = id_to_json_url.get(pid)
                opening_hours_val = id_to_opening_hours.get(pid)

                # Default include = False; apply filters
                include_row = True

                # Optional filter by json type (disabled by default for simple searches)
                if filter_by_json_type and self.poi_types:
                    if poi_json_types:
                        desired_list = [str(t).strip().lower() for t in self.poi_types]
                        candidate_list = [str(t).strip().lower() for t in (poi_json_types if isinstance(poi_json_types, list) else [poi_json_types])]
                        match = any(any(d in c for c in candidate_list) for d in desired_list)
                        if not match:
                            include_row = False
                            type_miss += 1
                    else:
                        # If filtering is enabled but we couldn't extract json type, keep to avoid over-dropping
                        pass

                # Optional filter by polygon containment (disabled by default)
                if filter_by_polygon and include_row and poly_union is not None:
                    if lat is not None and lng is not None:
                        try:
                            from shapely.geometry import Point
                            pt = Point(float(lng), float(lat))
                            if not pt.within(poly_union):
                                include_row = False
                                polygon_miss += 1
                        except Exception:
                            include_row = False
                            polygon_miss += 1
                    else:
                        include_row = False
                        polygon_miss += 1

                if include_row:
                    row_out = list(r)
                    # Ensure capacity for existing lat/lng indices if present
                    if coords_in_header and lat_idx is not None and lng_idx is not None:
                        while len(row_out) <= max(lat_idx, lng_idx):
                            row_out.append('')
                        row_out[lat_idx] = lat
                        row_out[lng_idx] = lng
                    else:
                        # Append missing lat/lng
                        if 'latitude' not in header:
                            row_out.append(lat)
                        if 'longitude' not in header:
                            row_out.append(lng)
                    # Append or set json_url and opening_hours
                    # Ensure row matches new_header length
                    while len(row_out) < len(new_header) - 2:  # before json_url and opening_hours
                        row_out.append('')
                    # Append json_url and opening_hours at the end
                    row_out.append(json_url_val)
                    row_out.append(opening_hours_val)
                    new_rows.append(row_out)
                    kept_count += 1

            if kept_count == 0:
                logger.warning(
                    f"Enrichment produced 0 rows; keeping original file unchanged: {output_path}. "
                    f"Stats(type_miss={type_miss}, polygon_miss={polygon_miss}, fetched={total_fetched}, total_rows={len(rows)})"
                )
            else:
                tmp_path = output_path + '.tmp'
                with open(tmp_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerows(new_rows)
                    f.flush()
                    if hasattr(os, 'fsync'):
                        os.fsync(f.fileno())
                os.replace(tmp_path, output_path)
                logger.info(
                    f"Output CSV enriched and filtered: kept={kept_count}, type_miss={type_miss}, polygon_miss={polygon_miss}, fetched={total_fetched}"
                )

        except Exception as e:
            logger.error(f"Error enriching output with coordinates: {e}")
            raise