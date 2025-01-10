import os

class Config:
    """
    Configuration class for the Google Places API scraper.
    
    This class manages all configuration settings, including file paths
    and constants used throughout the application.
    """

    def __init__(self, base_dir):
        """
        Initialize the Config object with the base directory.

        Args:
            base_dir (str): The base directory for the project.
        """
        self.base_dir = base_dir
        
        # Define file paths relative to the base directory
        self.api_keys_file = os.path.join(base_dir, "data", "api_keys.txt")
        self.output_json_file = os.path.join(base_dir, "output", "api_usage.json")
        self.output_csv_file = os.path.join(base_dir, "output", "results.csv")
        self.txt_folder = os.path.join(base_dir, "data", "chunks_new")
        self.completed_poi_types_file = os.path.join(base_dir, "output", "completed_search_poi_types.txt")
        self.log_file = os.path.join(base_dir, "logs", "scraper_logs.txt")
        
        # Constants
        self.MAX_CONCURRENT_REQUESTS = 250  # Maximum number of concurrent API requests

    def create_directories(self):
        """
        Create necessary directories for output files if they don't exist.
        
        This method ensures that all required directories are available
        before the application starts writing files.
        """
        directories = [
            os.path.dirname(self.output_json_file),
            os.path.dirname(self.output_csv_file),
            os.path.dirname(self.log_file)
        ]
        for directory in directories:
            os.makedirs(directory, exist_ok=True)
