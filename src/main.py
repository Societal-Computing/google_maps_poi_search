import asyncio
import yaml
import logging
import time
from pathlib import Path
from src.api_key_manager import KeyManager
from src.data_manager import DataManager
from src.bbox_utils import BBoxUtils
from src.poi_searcher import POISearcher

def load_config():
    config_path = Path(__file__).parent.parent / 'data' / 'config.yaml'
    with open(config_path) as f:
        return yaml.safe_load(f)

def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="[%(levelname)s] %(message)s",
        handlers=[logging.StreamHandler()]
    )

async def main():
    setup_logging()
    config = load_config()
    
    data_manager = DataManager(config)
    data_manager.initialize_output_csv()
    
    api_keys = data_manager.load_file(config['paths']['api_keys'])
    if not api_keys:
        raise Exception("No API keys found")
    
    key_manager = KeyManager(
        api_keys=api_keys,
        cooldown_time=config['api']['cooldown_time']
    )
    
    bbox_utils = BBoxUtils()
    
    start_time = time.time()

    searcher = POISearcher(
        config=config,
        data_manager=data_manager,
        key_manager=key_manager,
        bbox_utils=bbox_utils
    )
    
    try:
        await searcher.run()
    except Exception as e:
        logging.error(f"Error during processing: {e}")
    finally:
        # Save API requests before closing session
        # data_manager.save_api_requests(key_manager.api_requests_count)
        
        # Explicitly wait for session closure
        # if searcher.session:
        #     if not searcher.session.closed:
        #         await searcher.session.close()
        
        end_time = time.time()
        total_time = round(end_time - start_time, 2)
        logging.info(f"Total execution time: {total_time} seconds")

if __name__ == "__main__":
    asyncio.run(main())