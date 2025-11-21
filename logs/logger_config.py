import logging
import os
from datetime import datetime

def setup_logger(log_filename):
    """
    Creates a logger that writes to logs/<log_filename>
    Returns a configured logging object.
    """

    # Path to logs folder
    LOG_DIR = os.path.dirname(os.path.abspath(__file__))
    os.makedirs(LOG_DIR, exist_ok=True)

     # Full path to log file   
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")                                                  
    LOG_PATH = os.path.join(LOG_DIR, f'{log_filename}{timestamp}_log.txt')

    # Configure logging
    logging.basicConfig(
        filename=LOG_PATH,
        level=logging.INFO,
        format="%(asctime)s - %(module)s- %(levelname)s - %(message)s"
    )

    # Create and return logger
    logger = logging.getLogger(log_filename)
    return logger

