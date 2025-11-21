from Config.settings import *
# from etl.extract_knmi import download_knmi_csv
from logs.logger_config import setup_logger 
from etl.transform_data import transformed_data
from etl.load import save_monthly_data
if __name__ =='__main__':
    transformed_df = transformed_data()
    