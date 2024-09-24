import pandas as pd 
import numpy as np
import requests
from io import StringIO

url = "https://www.ncei.noaa.gov/data/global-hourly/access/2024/01025099999.csv"

responce = requests.get(url)
if(responce.status_code == 200):
    data = StringIO(responce.text)
    df = pd.read_csv(data)
    print(df.head())
    print(df.dtypes)

    df = df[df['SLP'].notna() & (df['SLP'] != '99999,9')]
    df['SLP'] = df['SLP'].apply(lambda x: float(x.split(',')[0]) / 10)
    df['TMP_C'] = df['TMP'].apply(lambda x: float(x.split(',')[0]) / 10)
    df['Temp_Category'] = pd.cut(df['TMP_C'], bins=[-float('inf'), 0, 20, float('inf')], labels=['низька', 'середня', 'висока'])
    df['DEW_C'] = df['DEW'].apply(lambda x: float(x.split(',')[0]) / 10)
    df['DATE'] = pd.to_datetime(df['DATE'])
    df['Month'] = df['DATE'].dt.month
    df['Temp_Dew_Diff'] = df['TMP_C'] - df['DEW_C']
    print(df)
    monthly_summary = df.groupby('Month').agg({
        'Temp_Dew_Diff': 'mean'
    }).reset_index()

    print(monthly_summary)
else:
    print("No data")
