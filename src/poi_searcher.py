import asyncio
import aiohttp
import logging

class POISearcher:
    """
    A class to handle the search and processing of Points of Interest (POIs) in a given area.

    Attributes:
        config (dict): Configuration settings.
        data_manager (DataManager): Instance to manage data operations (e.g., saving results).
        key_manager (KeyManager): Instance to manage API keys and rate limiting.
        bbox_utils (BBoxUtils): Instance to handle bounding box utilities.
        task_queue (asyncio.Queue): Queue to manage tasks for POI searching.
        searched_areas (set): Set to track already searched areas.
        saved_place_ids (set): Set to track the IDs of saved POIs.
        locks (dict): Lock objects to synchronize access to shared data structures.
        initial_bbox (Shapely Box): Initial bounding box to start the search.
        initial_tasks (set): Set of initial POI types to process.
        session (aiohttp.ClientSession): The HTTP session used for API requests.
    """
    
    def __init__(self, config, data_manager, key_manager, bbox_utils):
        """
        Initializes the POISearcher with necessary configurations and resources.

        Args:
            config (dict): Configuration settings for the search.
            data_manager (DataManager): Instance to manage data operations.
            key_manager (KeyManager): Instance to manage API keys.
            bbox_utils (BBoxUtils): Utility class for bounding box operations.
        """
        self.config = config
        self.data_manager = data_manager
        self.key_manager = key_manager
        self.bbox_utils = bbox_utils
        self.task_queue = asyncio.Queue()  # Queue for tasks to process POIs
        self.searched_areas = set()  # Set to keep track of areas already searched
        self.saved_place_ids = set()  # Set to track POIs that have been saved
        self.locks = {
            "searched": asyncio.Lock(),  # Lock for managing searched areas
            "results": asyncio.Lock()    # Lock for managing saved results
        }
        self.initial_bbox = None  # Placeholder for the initial bounding box
        self.initial_tasks = set()  # Placeholder for initial POI tasks
        self.session = None  # Placeholder for the HTTP session

    async def search_pois(self, bbox, poi_type):
        """
        Searches for points of interest (POIs) in a given bounding box for a specific POI type.

        Args:
            bbox (Shapely Box): The bounding box representing the area to search.
            poi_type (str): The type of POI to search for.

        Returns:
            list: A list of tuples containing POI IDs and types, or an empty list if no results.
        """
        url = self.config['api']['base_url']  # The API endpoint for searching POIs
        bounds = bbox.bounds
        data = {
            "textQuery": poi_type,
            "locationRestriction": {
                "rectangle": {
                    "low": {"latitude": bounds[1], "longitude": bounds[0]},
                    "high": {"latitude": bounds[3], "longitude": bounds[2]}
                }
            }
        }
        
        # Retry logic in case of API failure or rate limit
        for attempt in range(self.config['api']['max_retries']):
            key = await self.key_manager.get_key()  # Get an API key
            self.key_manager.api_requests_count[key] += 1  # Increment request count
            headers = {
                "Content-Type": "application/json",
                "X-Goog-Api-Key": key,
                "X-Goog-FieldMask": self.config['api']['field_mask']
            }
            try:
                async with self.session.post(url, headers=headers, json=data) as resp:
                    if resp.status == 200:
                        results = await resp.json()
                        return [(p["id"], poi_type) for p in results.get("places", [])]
                    if resp.status == 429:
                        # Rate limit exceeded, handle backoff
                        self.key_manager.mark_rate_limit(key)
                        await asyncio.sleep(self.config['api']['base_backoff'] ** attempt)
                    else:
                        break
            except Exception as e:
                # Retry after failure
                await asyncio.sleep(self.config['api']['base_backoff'] ** attempt)
        
        return []  # Return empty list if no results or failed retries

    async def process_poi(self, bbox, poi_type):
        """
        Processes the search results for a given bounding box and POI type, saving new POIs
        and dividing the area into smaller quadrants if necessary.

        Args:
            bbox (Shapely Box): The bounding box representing the area to process.
            poi_type (str): The type of POI to process.
        """
        normalized_coords = self.bbox_utils.normalize_coords(bbox.bounds,
                    self.config['processing']['coord_precision'])
        coord_key = (normalized_coords, poi_type)
        
        # Check if the area has already been searched
        async with self.locks["searched"]:
            if coord_key in self.searched_areas:
                return
            self.searched_areas.add(coord_key)

        # Search for POIs in the area
        results = await self.search_pois(bbox, poi_type)

        # Save new POIs if they haven't been saved before
        async with self.locks["results"]:
            new_pois = [p for p in results if p[0] not in self.saved_place_ids]
            if new_pois:
                self.saved_place_ids.update(p[0] for p in new_pois)
                self.data_manager.save_results(new_pois)
                logging.info(f"POI type '{poi_type}': Saved {len(new_pois)} new POIs.")

        # Track the initial tasks progress
        if (self.initial_bbox and 
            bbox.bounds == self.initial_bbox.bounds and 
            poi_type in self.initial_tasks):
            self.data_manager.update_progress()
            self.initial_tasks.remove(poi_type)

        # Split the bounding box into smaller quadrants if the number of results exceeds threshold
        if len(results) > self.config['processing']['threshold']:
            longest_side = self.bbox_utils.get_longest_side(bbox)
            overlap = self.bbox_utils.calculate_dynamic_overlap(longest_side)
            quadrants = self.bbox_utils.divide_box(bbox, overlap)
            await asyncio.gather(*[self.enqueue_task(q, poi_type) for q in quadrants])

    async def enqueue_task(self, bbox, poi_type):
        """
        Enqueues a new task to search for POIs in a specified bounding box and POI type.

        Args:
            bbox (Shapely Box): The bounding box for the new task.
            poi_type (str): The POI type for the new task.
        """
        await self.task_queue.put((bbox, poi_type))  # Add task to the queue
        self.data_manager.log_queue_task(bbox, poi_type)  # Log the enqueued task

    async def worker(self):
        """
        Worker function that processes tasks from the task queue asynchronously.

        Continuously processes tasks until the queue is empty.
        """
        while True:
            bbox, poi_type = await self.task_queue.get()
            try:
                await self.process_poi(bbox, poi_type)  # Process the POI search task
            finally:
                self.task_queue.task_done()  # Mark the task as done

    async def run(self):
        """
        Main entry point to start the POI search process. Initializes the session,
        loads the initial bounding box, and starts workers to process POI search tasks.

        It also logs progress and ensures all tasks are completed before shutting down.
        """
        self.session = aiohttp.ClientSession()  # Initialize the HTTP session
        try:
            # Load the initial bounding box from a geojson file
            self.initial_bbox = self.data_manager.load_geojson(
                self.config['paths']['city_geojson']
            )
            logging.info(f"Initial bbox loaded: {self.initial_bbox.bounds}")

            # Load the POI types to search for
            poi_types = self.data_manager.load_file(
                self.config['paths']['poi_types']
            )
            self.initial_tasks = set(poi_types)  # Track the initial tasks for progress
            self.data_manager.create_progress_bar(len(poi_types))  # Create a progress bar

            # Create worker tasks for handling POI search in parallel
            workers = [
                asyncio.create_task(self.worker()) 
                for _ in range(len(self.key_manager.keys) * self.config['processing']['workers_per_key'])
            ]

            # Enqueue the initial search tasks for all POI types
            for poi_type in poi_types:
                await self.enqueue_task(self.initial_bbox, poi_type)

            # Wait for all tasks to be processed
            await self.task_queue.join()

            # Cancel and wait for workers to finish
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            # Save API requests count before closing session
            self.data_manager.save_api_requests(self.key_manager.api_requests_count)

        finally:
            await self.session.close()  # Close the HTTP session after all tasks are complete
