import pandas as pd
import polars as pl
import numpy as np
from scipy.spatial.distance import cdist

def augment_school(df: pl.DataFrame):
    # Read school data
    dfschool = pd.read_csv('School/2019_2020_School.csv')
	# Convert to pandas
	dff = pd.DataFrame(df).transpose()
	dff.columns = df.columns
    
    # Create distance matrix (fillna(0) should make the NaNs irelevant)
    ## Change dfschool[["LONGITUDE","LATITUDE"]] whenever different column names!!
    dist_matrix = cdist(dff[["lng", "lat"]].fillna(0), dfschool[["LONGITUDE","LATITUDE"]].fillna(0))
    
    # Put data into original data (Add more columns if needed!)
    dff["Closest_school"] = pd.Series([dfschool["location_code"].iloc[np.argmin(x)] for x in dist_matrix])
    
	# Convert back to polars
    return pl.DataFrame(dff)
    


