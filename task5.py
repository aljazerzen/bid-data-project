# %%
import pandas as pd
import polars as pl
import plotnine as pn
import dask.dataframe as dd

# %%
from dask.distributed import Client

Client('tcp://127.0.0.1:37781')
# %%

df = dd.read_parquet('./data/2022.gz.parquet')

# %%
df = df[(df['Vehicle Year'] >= 1970) & (df['Vehicle Year'] <= 2022)]

X = df[[
  'Vehicle Year', 
  'cafe_count',
  'Closest_school_dist',
]]
# X = df[[
#   'Vehicle Body Type', 
#   'Vehicle Make', 
#   'Vehicle Year', 
#   'Borough Code', 
#   'cafe_count',
#   'Closest_school_dist',
#   'Vehicle Color'
# ]]
y = df['Violation Price']
# %%
from dask_ml.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(
  X.to_dask_array(lengths=True), 
  y.to_dask_array(lengths=True)
)
X_train

# %%

from dask_ml.linear_model import LinearRegression
from dask_ml.metrics import mean_squared_error
# %%
baseline = LinearRegression()
baseline.fit(X_train[:, []], y_train)
# %%
y_baseline = baseline.predict(X_test[:, []])
mse_baseline = mean_squared_error(y_test, y_baseline)
mse_baseline
# %%
lr = LinearRegression()
lr.fit(X_train, y_train)
# %%

y_predict = lr.predict(X_test).compute()
mse = mean_squared_error(y_test, y_predict)
mse




# %%

X = df[[
  'Vehicle Body Type', 
  'Vehicle Make', 
  'Vehicle Year', 
  'Borough Code', 
  'cafe_count',
  'Closest_school_dist',
  'Vehicle Color'
]]
y = df['Violation Price']
# %%
X_train, X_test, y_train, y_test = train_test_split(
  X.to_dask_array(lengths=True), 
  y.to_dask_array(lengths=True)
)
X_train

# %%

X_train
