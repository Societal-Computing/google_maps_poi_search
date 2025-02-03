import asyncio
import aiohttp
import logging
import csv

class POISearcher:
    def __init__(self, config, data_manager, key_manager, bbox_utils):
        self.config = config
        self.data_manager = data_manager
        self.key_manager = key_manager
        self.bbox_utils = bbox_utils
        self.task_queue = asyncio.Queue()
        self.searched_areas = set()
        self.saved_place_ids = set()
        self.locks = {
            "searched": asyncio.Lock(),
            "results": asyncio.Lock()
        }
        self.initial_bbox = None
        self.initial_tasks = set()
        self.session = None

    async def search_pois(self, bbox, poi_type):
        url = self.config['api']['base_url']
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
        for attempt in range(self.config['api']['max_retries']):
            key = await self.key_manager.get_key()
            self.key_manager.api_requests_count[key] += 1
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
                        self.key_manager.mark_rate_limit(key)
                        await asyncio.sleep(self.config['api']['base_backoff'] ** attempt)
                    else:
                        break
            except Exception as e:
                await asyncio.sleep(self.config['api']['base_backoff'] ** attempt)
        return []

    async def process_poi(self, bbox, poi_type):
        coord_key = (self.bbox_utils.normalize_coords(bbox.bounds, 
                    self.config['processing']['coord_precision']), poi_type)
        async with self.locks["searched"]:
            if coord_key in self.searched_areas:
                return
            self.searched_areas.add(coord_key)

        results = await self.search_pois(bbox, poi_type)

        async with self.locks["results"]:
            new_pois = [p for p in results if p[0] not in self.saved_place_ids]
            if new_pois:
                self.saved_place_ids.update(p[0] for p in new_pois)
                self.data_manager.save_results(new_pois)
                logging.info(f"POI type '{poi_type}': Saved {len(new_pois)} new POIs.")

        if (self.initial_bbox and 
            bbox.bounds == self.initial_bbox.bounds and 
            poi_type in self.initial_tasks):
            self.data_manager.update_progress()
            self.initial_tasks.remove(poi_type)

        if len(results) > self.config['processing']['threshold']:
            longest_side = self.bbox_utils.get_longest_side(bbox)
            overlap = self.bbox_utils.calculate_dynamic_overlap(longest_side)
            quadrants = self.bbox_utils.divide_box(bbox, overlap)
            await asyncio.gather(*[self.enqueue_task(q, poi_type) for q in quadrants])

    async def enqueue_task(self, bbox, poi_type):
        await self.task_queue.put((bbox, poi_type))
        self.data_manager.log_queue_task(bbox, poi_type)

    async def worker(self):
        while True:
            bbox, poi_type = await self.task_queue.get()
            try:
                await self.process_poi(bbox, poi_type)
            finally:
                self.task_queue.task_done()

    async def run(self):
        self.session = aiohttp.ClientSession()
        try:
            self.initial_bbox = self.data_manager.load_geojson(
                self.config['paths']['city_geojson']
            )
            logging.info(f"Initial bbox loaded: {self.initial_bbox.bounds}")

            poi_types = self.data_manager.load_file(
                self.config['paths']['poi_types']
            )
            self.initial_tasks = set(poi_types)
            self.data_manager.create_progress_bar(len(poi_types))

            workers = [
                asyncio.create_task(self.worker()) 
                for _ in range(len(self.key_manager.keys) * self.config['processing']['workers_per_key'])
            ]

            for poi_type in poi_types:
                await self.enqueue_task(self.initial_bbox, poi_type)

            await self.task_queue.join()
            for w in workers:
                w.cancel()
            await asyncio.gather(*workers, return_exceptions=True)

            # Save API requests count HERE before closing session
            self.data_manager.save_api_requests(self.key_manager.api_requests_count)

        finally:
            await self.session.close()