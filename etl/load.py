import os
import pandas as pd
# from Config.settings import *
df_cleaned = pd.read_csv('C:/Users/ec.raihana/Desktop/Assignment 1/Data/Processed/knmi_cleaned_.csv',index_col='datetime',parse_dates=True)
# print(df)
# print(type(df.index))

BASE_DIR = r"C:/Users/ec.raihana/Desktop/Assignment 1/Data/Final"
def save_monthly_data(df : pd.DataFrame) -> None:
    """
    Creates folder structure:
        Final/year=YYYY/month=MM/
    and saves each month's dataframe inside it.
    """

    # Extract year-month information
    df["year"] = df.index.year
    # print(df["year"])
    df["month"] = df.index.month

    years = df["year"].unique()

    for yr in years:
        df_year = df[df["year"] == yr]
        # print(df_year)

        # Create year folder
        year_folder = os.path.join(BASE_DIR, f"year={yr}")
        os.makedirs(year_folder, exist_ok=True)

        months = df_year["month"].unique()

        for m in months:
            df_month = df_year[df_year["month"] == m]

            # Create month folder
            month_folder = os.path.join(year_folder, f"month={m}")
            os.makedirs(month_folder, exist_ok=True)
            df_month.to_parquet(os.path.join(month_folder,"data.parquet"))

save_monthly_data(df_cleaned)



