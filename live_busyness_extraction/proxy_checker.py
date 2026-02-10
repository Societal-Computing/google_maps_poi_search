import os
import requests
import time
from datetime import datetime
import logging
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple
import glob

def setup_logging():
    """Setup logging with cleanup of old log files."""
    # Create logs directory if it doesn't exist
    logs_dir = 'logs'
    os.makedirs(logs_dir, exist_ok=True)
    
    # Get all existing log files
    log_files = glob.glob(os.path.join(logs_dir, 'proxy_check_*.log'))
    
    # If there are more than 2 existing log files, keep only the latest 2
    if len(log_files) > 2:
        # Sort files by creation time
        log_files.sort(key=os.path.getctime)
        # Delete all but the latest 2 files
        for old_file in log_files[:-2]:
            try:
                os.remove(old_file)
                print(f"Removed old log file: {old_file}")
            except Exception as e:
                print(f"Failed to remove old log file {old_file}: {e}")
    
    # Setup new log file
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = os.path.join(logs_dir, f'proxy_check_{timestamp}.log')
    
    # Configure the logger
    logger = logging.getLogger('proxy_checker')
    logger.setLevel(logging.INFO)
    
    # Create file handler
    file_handler = logging.FileHandler(log_file)
    file_handler.setLevel(logging.INFO)
    
    # Create console handler
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    
    # Create formatter
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    
    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    return logger

logger = setup_logging()

class ProxyChecker:
    def __init__(self, proxy_username: str, proxy_password: str):
        self.proxy_username = proxy_username
        self.proxy_password = proxy_password
        self.test_url = "https://www.google.com/maps"
        self.timeout = 10

    def format_proxy(self, proxy: str) -> str:
        """Format proxy with authentication."""
        if not proxy.startswith("http://") and not proxy.startswith("https://"):
            proxy = f"http://{proxy}"
        return f"http://{self.proxy_username}:{self.proxy_password}@{proxy.split('://')[-1]}"

    def check_proxy(self, proxy: str) -> Tuple[str, bool]:
        """Check if a proxy is working."""
        try:
            formatted_proxy = self.format_proxy(proxy)
            proxies = {
                "http": formatted_proxy,
                "https": formatted_proxy
            }
            
            start_time = time.time()
            response = requests.get(
                self.test_url,
                proxies=proxies,
                timeout=self.timeout
            )
            response_time = time.time() - start_time
            
            if response.status_code == 200:
                logger.info(f"Proxy {proxy} is working (response time: {response_time:.2f}s)")
                return proxy, True
            else:
                logger.warning(f"Proxy {proxy} returned status code {response.status_code}")
                return proxy, False
                
        except Exception as e:
            logger.error(f"Proxy {proxy} failed: {str(e)}")
            return proxy, False

    def check_proxies(self, proxy_file: str, max_workers: int = 10) -> List[str]:
        """Check multiple proxies in parallel."""
        # Read proxies from file
        with open(proxy_file, 'r') as f:
            proxies = [line.strip() for line in f if line.strip()]
        
        logger.info(f"Testing {len(proxies)} proxies...")
        
        working_proxies = []
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = [executor.submit(self.check_proxy, proxy) for proxy in proxies]
            for future in futures:
                proxy, is_working = future.result()
                if is_working:
                    working_proxies.append(proxy)
        
        logger.info(f"Found {len(working_proxies)} working proxies out of {len(proxies)}")
        return working_proxies

    def save_working_proxies(self, working_proxies: List[str], output_dir: str) -> str:
        """Save working proxies to a timestamped file and clean up old files except the latest one."""
        # Create output directory if it doesn't exist
        os.makedirs(output_dir, exist_ok=True)
        
        # Generate new timestamped filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(output_dir, f"working_proxies_{timestamp}.txt")
        
        # Get all existing proxy files
        existing_files = glob.glob(os.path.join(output_dir, "working_proxies_*.txt"))
        
        # If there are existing files, keep only the latest one
        if existing_files:
            # Sort files by creation time
            existing_files.sort(key=os.path.getctime)
            # Keep the latest file, delete all others
            for old_file in existing_files[:-1]:
                try:
                    os.remove(old_file)
                    logger.info(f"Removed old proxy file: {old_file}")
                except Exception as e:
                    logger.warning(f"Failed to remove old proxy file {old_file}: {e}")
        
        # Save the new working proxies
        with open(output_file, 'w') as f:
            for proxy in working_proxies:
                f.write(f"{proxy}\n")
        
        logger.info(f"Saved {len(working_proxies)} working proxies to {output_file}")
        return output_file

def main():
    import argparse
    
    # Get the directory where the script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Set default paths
    default_proxy_file = os.path.join(script_dir, 'input', 'proxies.txt')
    default_output_dir = os.path.join(script_dir, 'input', 'working_proxies')
    if not os.path.exists(default_output_dir):
        os.makedirs(default_output_dir, exist_ok=True)
    parser = argparse.ArgumentParser(description='Check proxy health and save working proxies')
    parser.add_argument('--proxy-file', default=default_proxy_file, help='File containing proxy list')
    parser.add_argument('--output-dir', default=default_output_dir, help='Directory to save working proxies')
    parser.add_argument('--proxy-username', required=True, help='Proxy username')
    parser.add_argument('--proxy-password', required=True, help='Proxy password')
    parser.add_argument('--max-workers', type=int, default=10, help='Maximum number of parallel workers')
    
    args = parser.parse_args()
    
    try:
        # Initialize checker
        checker = ProxyChecker(args.proxy_username, args.proxy_password)
        
        # Check proxies
        working_proxies = checker.check_proxies(args.proxy_file, max_workers=args.max_workers)
        
        # Save working proxies
        output_file = checker.save_working_proxies(working_proxies, args.output_dir)
        
        logger.info(f"Proxy check completed. Working proxies saved to: {output_file}")
        
    except Exception as e:
        logger.error(f"Error during proxy check: {e}")
        raise

if __name__ == "__main__":
    main() 