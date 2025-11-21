import pandas as pd
import numpy as np
# from etl.transform_data import derivative_features

#derivative_feature function call
df1 = pd.read_csv('C:/Users/ec.raihana/Desktop/Assignment 1/Data/Processed/knmi_cleaned_308_2020-01-01_to_2020-01-31.csv',index_col='datetime')
print(df1)
print(df1.dtypes)
df1.index = pd.to_datetime(df1.index)

#Aggregate Windspeed by Day
df1["date"] = df1.index.date
df1["gust_ms"] = df1["wind_gust_max_0_1ms"] / 10
threshold = int(input('enter the threshold value for gust'))
daily_summary = df1.resample("D").agg({
"wind_speed": ["min", "max", "mean"],
"gust_ms": lambda x: (x > threshold).sum()
})  
print('**********Daily Summary**********')
print(daily_summary)

#Computing Wind Energy Approximation
df1['wind_power'] = df1["wind_speed"]**3

#Calculating daily average wind power proxy
d_avg_wind_power = df1['wind_power'].resample("D").mean()
print('-----Daily avearge wind power-----')
print(d_avg_wind_power)

#Identifying the top 5 windiest days by energy
top_5_windiest_days = d_avg_wind_power.sort_values(ascending=False).head(5)
print('-----Top 5 windiest days-----')
print(top_5_windiest_days)

#Detecting  Multi-Hour Wind Calms
wind_calm_threshold = int(input('enter the consecutive calm hours threshold'))
datetime_list = []
streak = 0
runs = None
for idx,row in df1.iterrows():
    if row["wind_speed"] < 1:
        if streak ==0:
            runs = idx
            start_time = idx
        streak= streak + 1
        print('streak is',streak, 'windspeed is',row["wind_speed"])
        if streak==wind_calm_threshold:
            end_time = idx
            duration = end_time - start_time
            datetime_list.append({'runs':runs,'start_time':start_time,'end_time':end_time,'duration':duration})
            longest_datetime_list = sorted(datetime_list, key=lambda x: x['duration'], reverse=True)
            streak = 0
            runs = None 
    else:
    # Calm streak ended 
            streak = 0
            runs = None   
print("Calm periods are:", datetime_list)

#longest calm spell
print('the longest calm_spell is',longest_datetime_list[0])

#verifying
for k in datetime_list:
        start = k['start_time']
        stop = k['end_time']
        filtered_rows = df1[(df1.index>=start) & (df1.index<=stop)]
        print(filtered_rows["wind_speed"].mean())

        
print(df1[df1["wind_speed"]<1]['wind_speed'])
print(df1)

              
         












  