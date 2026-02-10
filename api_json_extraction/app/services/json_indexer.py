# app/services/json_indexer.py
import json
import pandas as pd
from datetime import datetime
import itertools
import os
import logging
from typing import Dict, List, Optional, Tuple, Any
import requests
import time
import random
from collections import defaultdict
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter

# Configure logging
logger = logging.getLogger(__name__)

def process_json_file(file_path: str) -> Tuple[bool, dict]:
    """Process a single JSON file and extract relevant data."""
    try:
        with open(file_path, 'r') as f:
            data = json.load(f)
            
        # Extract basic information
        result = {
            'file_name': os.path.basename(file_path),
            'timestamp': datetime.now().isoformat(),
            'data': data
        }
        
        return True, result
    except Exception as e:
        logger.error(f"Error processing file {file_path}: {e}")
        return False, {'error': str(e)}

def batch_process(input_dir: str, output_csv: str = None):
    """Process all JSON files in a directory and save results to CSV."""
    try:
        results = []
        for filename in os.listdir(input_dir):
            if filename.endswith('.json'):
                file_path = os.path.join(input_dir, filename)
                success, result = process_json_file(file_path)
                if success:
                    results.append(result)
        
        if output_csv and results:
            df = pd.DataFrame(results)
            df.to_csv(output_csv, index=False)
            logger.info(f"Saved {len(results)} results to {output_csv}")
            
        return results
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        return []
    