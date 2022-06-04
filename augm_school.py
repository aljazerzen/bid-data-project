import pandas as pd
import numpy as np
from scipy.spatial.distance import cdist

def augment_school(df: pd.DataFrame):
    # Read school data
    dfschool = pd.read_csv('School/2019_2020_School.csv')
    
    # Create distance matrix (fillna(0) should make the NaNs irelevant)
    ## Change dfschool[["LONGITUDE","LATITUDE"]] whenever different column names!!
    dist_matrix = cdist(df[["lng", "lat"]].fillna(0), dfschool[["LONGITUDE","LATITUDE"]].fillna(0))
    
    # Put data into original data (Add more columns if needed!)
    dfdata["Closest_school"] = pd.Series([dfschool["location_code"].iloc[np.argmin(x)] for x in dist_matrix])
    
    return dfdata
    


