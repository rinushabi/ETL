import pandas as pd
import numpy as np

from Config.settings import station_id,start_date,end_date
# Import logger function
from logs.logger_config import setup_logger

# Setup logger for extract step
logger = setup_logger(__name__)

def transformed_data()->None:
    '''function to transform data'''
    df_final = []
    for st in station_id:

        logger.info(f"Transformation started for station {st}")

        input_file = f'C:/Users/ec.raihana/Desktop/Assignment 1/Data/Raw/knmi_{st}_{start_date}_to_{end_date}.csv'
        output_file = f'C:/Users/ec.raihana/Desktop/Assignment 1/Data/Processed/knmi_cleaned_.csv'
        
        logger.info(f"Reading input file: {input_file}")

        colnames = [
        "STN","YYYYMMDD","HH","DD","FH","FF","FX","T","T10N","TD",
        "SQ","Q","DR","RH","P","VV","N","U","WW","IX","M","R","S","O","Y"]
        
        try:
            df = pd.read_csv(input_file, comment='#',header = None,names = colnames,sep = ',')
            logger.info("File read successfully")
            print(df.head())
            print(df.describe())
        

        # Standardize column names
            df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
            logger.info("Column names standardized")
            print(df.columns)

        # Datetime function call
            df = parse_datetime(df)
            logger.info("Datetime parsing completed")
            
            #Checking missing values
            print(df.isna().sum())  #no null values
            logger.info(f"This is the count of missing values before cleaning:")
            
        #Rename function call
            df = renaming(df)
            logger.info("Columns renamed as per mapping")

            df = df.sort_values(by = 'datetime')

            #Drop unnecessary columns and set datetime column as index column
            df.drop(columns=['date_yyyymmdd','hour_utc'],inplace = True)
            df.set_index(df.columns[-1], inplace=True)
            print(df.head())
            logger.info("Unnecessary columns dropped and datetime set as index")
            
            #change column type to numeric
            column = ['wind_direction_deg','wind_speed_hourly_mean_0_1ms','wind_speed_10min_mean_0_1ms','wind_gust_max_0_1ms']
            for col in column:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            print(df.dtypes)
            logger.info("Converted selected columns to numeric")
        
            #derivative function call
            df = derivative_features(df)
            logger.info("Derivative features added")

            

            #Creating new dataframe which includes columns related to wind except delta wind column
            df1=df[['station_id', 'wind_direction_deg', 'wind_speed_hourly_mean_0_1ms',
            'wind_speed_10min_mean_0_1ms', 'wind_gust_max_0_1ms', 'wind_speed',
            'wind_speed_roll3', 'wind_spike']]
            
            logger.info(f"Transformation completed for station {st}")

            
            df_final.append(df1)
        

        except Exception as e:
            logger.exception(f"Error reading file: {input_file}:{e}")
            raise  
    df_final = pd.concat(df_final)
    print('The transformed dataframe is',df_final)
    df_final.to_csv(output_file)
    print("Saved cleaned file to:", output_file)
    # return df_final         


#date time function definition
def parse_datetime(df):
    '''Function to Parse date + hour into datetime'''
    logger.info("Parsing datetime started")
    try:
        print(df['hh'])
        df["hh"] = pd.to_timedelta(df["hh"],unit = 'h')
        print(df['hh'])
        df["datetime"] = pd.to_datetime(df["yyyymmdd"].astype(str),format="%Y%m%d") + df["hh"]
        logger.info("Datetime parsing successful")
    except Exception:
        logger.exception("Error during datetime parsing")
        raise
    return df

def renaming(df):
    '''Function to rename important columns''' 
    logger.info("Renaming columns")
    newname = {
    "stn": "station_id",
    "yyyymmdd": "date_yyyymmdd",
    "hh": "hour_utc",
    "dd": "wind_direction_deg",
    "fh": "wind_speed_hourly_mean_0_1ms",
    "ff": "wind_speed_10min_mean_0_1ms",
    "fx": "wind_gust_max_0_1ms",
    "t": "temperature_0_1c",
    "t10n": "temp_min_10cm_last6h_0_1c",
    "td": "dew_point_0_1c",
    "sq": "sunshine_duration_0_1h",
    "q": "global_radiation_j_cm2",
    "dr": "precipitation_duration_0_1h",
    "rh": "precipitation_amount_0_1mm",
    "p": "air_pressure_0_1hpa",
    "vv": "visibility_code",
    "n": "cloud_cover_oktas",
    "u": "humidity_percent",
    "ww": "weather_code",
    "ix": "weather_code_indicator",
    "m": "fog_occurrence",
    "r": "rain_occurrence",
    "s": "snow_occurrence",
    "o": "thunder_occurrence",
    "y": "ice_occurrence"
}

    df = df.rename(columns=newname)
    logger.info("Columns renamed successfully")
    return df
 
def derivative_features(df):
    '''Function to derive new features'''
    logger.info("Computing derivative features")
    try:
        print(df["wind_speed_hourly_mean_0_1ms"].describe())
        df["wind_speed"] = (df["wind_speed_hourly_mean_0_1ms"] / 10).round(3)
        df["wind_speed_roll3"] = df["wind_speed"].rolling(window=3).mean()
        
        #Sudden Wind Speed Spikes
        #delta_wind = wind_prev_hour - wind_this_hour
        df["delta_wind"] = df["wind_speed_hourly_mean_0_1ms"].diff()
        print(df["delta_wind"])

        #Flag hours where abs(delta_wind) > 5m/s
        df["wind_spike"] = df["delta_wind"].abs() > 5
        print(df["wind_spike"])
        logger.info("Derivative features computed successfully")

    except Exception:
        logger.exception("Error computing derivative features")
        raise

    return df