import requests
import os

# Import logger function
from logs.logger_config import setup_logger

#Import settings module
from Config.settings import *

# Setup logger for extract step
logger = setup_logger(__name__)


# Folder to save raw files
os.makedirs(OUTPUT_DIR_EXTRACT, exist_ok=True)
def download_knmi_csv(station_id : int, start_dt : str, end_dt : str)-> None:
    """
    function to extract data
    
    """
    logger.info(f"Starting download for station {station_id}")
    datacsv = {
        "stns": station_id,
        "start": start_dt,
        "end": end_dt,
        # select variables; this example uses ALL
        "vars": "ALL"
    }
    print(f"Downloading station {station_id}...")
    try:
        response = requests.post(url, data=datacsv)
        if response.status_code == 200:
            filename = f"knmi_{station_id}_{start_dt}_to_{end_dt}.csv"
            filepath = os.path.join(OUTPUT_DIR_EXTRACT, filename)

            with open(filepath, "w", newline="") as f:
                f.write(response.text)
            logger.info(f"File saved: {filepath}")
        # print("Saved CSV:", filepath)
        else:
            logger.error(f"Failed downloading {station_id}: {response.status_code}")
            # print("Failed:", response.status_code, response.text)
    except Exception as e:
        logger.exception(f"Unexpected error while downloading {station_id}")

for st in station_id:
    download_knmi_csv(st, start_date, end_date)