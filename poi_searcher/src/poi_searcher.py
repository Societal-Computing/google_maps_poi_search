import asyncio
import aiohttp
import logging

logger = logging.getLogger(__name__)

class POISearcher:
    """
    Class responsible for managing and executing the search for Points of Interest (POIs)
    based on given bounding boxes and POI types using asynchronous API requests.

    Attributes:
        data_manager (DataManager): Manages data-related tasks and stores results.
        api_key_manager (APIKeyManager): Handles API key management and rate-limiting.
        bbox_utils (BBoxUtils): Utility class for bounding box operations.
        config (dict): Configuration dictionary loaded from the config file.
        task_queue (asyncio.Queue): Queue to hold tasks for processing.
        searched_areas (set): Set of areas that have already been searched.
        saved_place_ids (set): Set of place IDs that have already been saved.
        locks (dict): Locks to ensure thread-safety during asynchronous operations.
        initial_bbox (BBox): The initial bounding box for the search.
        initial_tasks (set): Set of initial POI types to be processed.
    """
    def __init__(self, data_manager, api_key_manager, bbox_utils, config):
        """
        Initializes the POISearcher class with the given parameters.

        Args:
            data_manager (DataManager): The data manager instance.
            api_key_manager (APIKeyManager): The API key manager instance.
            bbox_utils (BBoxUtils): The bounding box utilities instance.
            config (dict): The configuration dictionary.
        """
        self.data_manager = data_manager
        self.api_key_manager = api_key_manager
        self.bbox_utils = bbox_utils
        self.config = config
        self.task_queue = asyncio.Queue()
        self.searched_areas = set()
        self.saved_place_ids = set()
        self.locks = {
            'searched': asyncio.Lock(),
            'results': asyncio.Lock()
        }
        self.initial_bboxes = data_manager.initial_bboxes
        self.initial_tasks = set(data_manager.poi_types)

    async def enqueue_task(self, bbox, poi_type):
        """
        Enqueues a new task for processing the search of POIs in the specified bounding box.

        Args:
            bbox (BBox): The bounding box for the POI search.
            poi_type (str): The type of POI to search for.
        """
        await self.task_queue.put((bbox, poi_type))
        self.data_manager.log_queue_task(bbox, poi_type)

    async def search_pois(self, bbox, poi_type):
        """
        Makes an asynchronous API request to search for POIs within the given bounding box.

        Args:
            bbox (BBox): The bounding box for the POI search.
            poi_type (str): The type of POI to search for.

        Returns:
            list: A list of POIs with their ID and type.
        """
        url = self.config['api']['base_url']
        bounds = bbox.bounds
        data = {
            'textQuery': poi_type,
            # 'strictTypeFiltering': False,
            # 'rankPreference': 'DISTANCE',
            # 'openNow': False,
            'locationRestriction': {
                'rectangle': {
                    'low': {'latitude': bounds[1], 'longitude': bounds[0]},
                    'high': {'latitude': bounds[3], 'longitude': bounds[2]}
                }
            }
        }
        
        max_retries = self.config['api']['max_retries']
        base_backoff = self.config['api']['base_backoff']

        # Retry logic with exponential backoff
        for attempt in range(max_retries):
            key = await self.api_key_manager.get_key()
            self.api_key_manager.api_requests_count[key] += 1

            headers = {
                'Content-Type': 'application/json',
                'X-Goog-Api-Key': key,
                'X-Goog-FieldMask': self.config['api']['field_mask']
            }
            try:
                async with aiohttp.ClientSession() as session:
                    async with session.post(url, headers=headers, json=data) as resp:
                        if resp.status == 200:
                            results = await resp.json()
                            return [(p['id'], poi_type) for p in results.get('places', [])]
                        
                        # Handle transient errors (retry)
                        elif resp.status in {408, 429, 500, 502, 503, 504}:
                            if resp.status == 429:
                                # Rate limit exceeded, mark the key and wait before retrying
                                self.api_key_manager.mark_rate_limit(key)
                            logger.warning(f"API Key: {key} | Transient error {resp.status}. Retrying in {base_backoff ** attempt} seconds")
                            await asyncio.sleep(base_backoff ** attempt)

                        # Handle permanent errors (exit early)
                        elif resp.status == 400:
                            logger.error(f"API Key: {key} | 400 Bad Request: Invalid parameters.")
                            return []
                        elif resp.status == 401:
                            logger.error(f"API Key: {key} | 401 Unauthorized: API key is invalid or missing.")
                            self.api_key_manager.remove_key(key)
                            continue
                        elif resp.status == 403:
                            logger.error(f"API Key: {key} | 403 Forbidden: API key lacks necessary permissions.")
                            self.api_key_manager.remove_key(key)
                            continue                            
                        elif resp.status == 404:
                            logger.error(f"API Key: {key} | 404 Not Found: Invalid API endpoint.")
                            return []
                        
                        # Handle unexpected errors
                        else:
                            logger.error(f"API Key: {key} | Unhandled API error {resp.status}.")
                            return []

            except aiohttp.ClientError as e:
                logger.error(f"API Key: {key} | Request failed: {e}")
                await asyncio.sleep(base_backoff ** attempt)
            except Exception as e:
                logger.error(f"API Key: {key} | Unexpected error: {e}")
                return []
        
        # After max retries, check if any keys are available. If not, stop execution.
        if not self.api_key_manager.keys:
            logger.error("Max retries reached. No available API keys left. Stopping execution.")
            return []

    async def process_poi(self, bbox, poi_type):
        """
        Processes the search for POIs in the given bounding box and POI type.

        Args:
            bbox (BBox): The bounding box for the POI search.
            poi_type (str): The type of POI to search for.
        """
        coord_precision = self.config['processing']['coord_precision']
        normalized_coords = self.bbox_utils.normalize_coords(bbox.bounds, coord_precision)
        coords_key = (normalized_coords, poi_type)

        # Lock the search area to ensure no duplicate searches for the same coordinates
        async with self.locks['searched']:
            if coords_key in self.searched_areas:
                return
            self.searched_areas.add(coords_key)

        # Perform the POI search and retrieve results
        results = await self.search_pois(bbox, poi_type)

        if results: # Only acquire locks if we have results to save
            # Separate lock for results processing
            async with self.locks['results']:
                new_pois = [p for p in results if p[0] not in self.saved_place_ids]
                if new_pois:
                    self.saved_place_ids.update(p[0] for p in new_pois)
                    await self.data_manager.save_results(new_pois)
                    logging.info(f"POI type '{poi_type}': Saved {len(new_pois)} new POIs.")
                    
        # # Update progress for initial tasks
        # if (bbox.bounds == self.initial_bboxes.bounds and 
        #     poi_type in self.initial_tasks):
        #     self.data_manager.update_progress()
        #     self.initial_tasks.remove(poi_type)
        #     logger.info(f"POI type '{poi_type}' completed.")

        # Update progress if initial task
        if (bbox.bounds in [bbox.bounds for bbox in self.initial_bboxes] and
            poi_type in self.initial_tasks):
            self.data_manager.update_progress()
            self.initial_tasks.remove(poi_type)
            logger.info(f"POI type '{poi_type}' completed.")

        # Split the bounding box if the result size exceeds threshold
        if len(results) >= self.config['processing']['threshold']:
            divided_boxes = self.bbox_utils.divide_box(bbox)
            for box_ in divided_boxes:
                await self.enqueue_task(box_, poi_type)

    async def worker(self):
        """
        Worker function that continuously processes tasks from the task queue.
        It handles POI searches and ensures proper error handling.
        """
        while True:
            bbox, poi_type = await self.task_queue.get()
            try:
                await self.process_poi(bbox, poi_type)
            except Exception as e:
                logger.error(f"Error processing task: {e}")
            finally:
                self.task_queue.task_done()

    async def run(self):
        """
        Starts the POI search process, initializing workers and enqueuing initial tasks.
        It also waits for all tasks to complete and ensures proper shutdown.
        """
        if not self.api_key_manager.keys:
            logger.error("No API keys available")
            return
        if not self.data_manager.poi_types:
            logger.error("No POI types to search")
            return

        # Create worker tasks based on available API keys
        workers = [
            asyncio.create_task(self.worker()) 
            for _ in range(len(self.api_key_manager.keys) * self.config['processing']['workers_per_key'])
        ]

        # # Enqueue the initial search tasks for each POI type
        # for poi_type in self.data_manager.poi_types:
        #     await self.enqueue_task(self.initial_bboxes, poi_type)

        # Enqueue initial tasks for each exploded polygon
        for bbox in self.initial_bboxes:
            for poi_type in self.data_manager.poi_types:
                await self.enqueue_task(bbox, poi_type)

        # Initialize the progress bar for tracking
        self.data_manager.initialize_progress_bar()

        try:
            # Wait until all tasks are completed
            await self.task_queue.join()
        finally:
            # Cancel worker tasks after completion
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            # Close the progress bar and log completion
            self.data_manager.close_progress_bar()
            
            logger.info("POI search completed")   

            # Use synchronous save
            self.api_key_manager.save_api_requests()
            logger.info("API requests saved successfully") 
