import pandas as pd
import polars as pl
import numpy as np
from scipy.spatial.distance import cdist

from dask.distributed import Client
import dask.dataframe as dd
client = Client(address="tcp://127.0.0.1:37781")
print(client)

def augment_school(df: pl.DataFrame):

    # Read school data
    dfschool = pd.read_csv('data/School/2019_2020_School.csv')
    dfschool_loc = dfschool[["LONGITUDE","LATITUDE"]].fillna(0)

    def augment_chunk(df: pd.DataFrame):
        
        # Create distance matrix (fillna(0) should make the NaNs irelevant)
        dist_matrix = cdist(df[["lng", "lat"]].fillna(0), dfschool_loc)
        
        # Put data into original data (Add more columns if needed!)
        arg_min = np.argmin(dist_matrix, axis=1)
        closest_schools = dfschool.iloc[arg_min].reset_index()
        df = (df
            .reset_index()
            .assign(Closest_school_dist = np.min(dist_matrix, axis=1),)
            .assign(Closest_school_code = closest_schools.location_code)
            .assign(Closest_school_name = closest_schools.location_name)
        )
        return df

    ddf = dd.from_pandas(df.to_pandas(), chunksize=10000)
    res = ddf.map_partitions(augment_chunk).compute()
    return pl.from_pandas(res)
