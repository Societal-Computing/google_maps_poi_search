import logging
import sys
from logging.handlers import RotatingFileHandler

class StreamToLogger:
    """
    Redirects stdout/stderr to the logging system.
    
    This class allows for capturing standard output and error streams
    and redirecting them to the logging system, ensuring all output
    is consistently formatted and stored.
    """

    def __init__(self, logger, log_level=logging.INFO):
        """
        Initialize the StreamToLogger.

        Args:
            logger: The logger object to use for logging.
            log_level: The logging level to use (default: logging.INFO).
        """
        self.logger = logger
        self.log_level = log_level

    def write(self, message):
        """
        Write a message to the logger.

        Args:
            message: The message to be logged.
        """
        if message.strip():  # Ignore empty messages
            self.logger.log(self.log_level, message.strip())

    def flush(self):
        """
        Flush the stream.

        This method is required for file-like objects, but
        does nothing as the logger handles its own flushing.
        """
        pass

def setup_logger(log_file, log_level='DEBUG'):
    """
    Configure logging with both file and console handlers.
    
    This function sets up a logger that writes to both a file and the console,
    and redirects stdout and stderr to the logger.
    
    Args:
        log_file (str): Path to the log file.
        log_level (str): Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL).
    
    Returns:
        logging.Logger: Configured logger object.
    """
    # Convert string log level to numeric value
    numeric_level = getattr(logging, log_level.upper(), logging.DEBUG)

    # Create logger
    logger = logging.getLogger('places_scraper')
    logger.setLevel(numeric_level)

    # Remove existing handlers to prevent duplicates
    if logger.hasHandlers():
        logger.handlers.clear()

    # File handler (writes logs to file)
    file_handler = RotatingFileHandler(
        log_file, 
        mode="w", 
        encoding="utf-8", 
        backupCount=5
    )
    file_handler.setFormatter(logging.Formatter(
        '[%(asctime)s] [%(levelname)s] %(message)s'
    ))
    file_handler.setLevel(numeric_level)

    # Console handler (prints logs to the terminal)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(logging.Formatter(
        '[%(levelname)s] %(message)s'
    ))
    console_handler.setLevel(numeric_level)

    # Add handlers to logger
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

    # Redirect stdout & stderr to logging
    sys.stdout = StreamToLogger(logger, logging.INFO)
    sys.stderr = StreamToLogger(logger, logging.ERROR)

    return logger
