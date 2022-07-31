import polars as pl
import pandas as pd

def augment_weather(df: pl.DataFrame, yyear):
    tmp = pd.DataFrame(df).transpose()
    tmp.columns = df.columns
    #print(tmp['time_of_ticket'].unique())

    wea = pd.read_csv("./data/Weather/weather_{0}.csv".format(yyear))
    wea = wea[['datetime', 'temp', 'humidity', 'snowdepth', 'windspeed', 'description']]
    wea['snowdepth'].fillna(0, inplace=True)
    
    wea['datetime'] = pd.to_datetime(wea['datetime']).dt.date
    
    tmp['datetime'] = tmp['time_of_ticket'].dt.date

    res = tmp.merge(wea, on='datetime', how='left')

    out = pl.DataFrame(res)
	
    return out