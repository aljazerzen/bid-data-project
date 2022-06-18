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
df.min().compute()
# %%
df.max().compute()

# %%
agg = { 'Violation Price': ['mean', 'std', 'count'] }

by_color = df.groupby('Vehicle Color').agg(agg).compute()
by_body_type = df.groupby('Vehicle Body Type').agg(agg).compute()
by_make = df.groupby('Vehicle Make').agg(agg).compute()
by_vehicle_year = df.groupby('Vehicle Year').agg(agg).compute()
by_borough = df.groupby('Borough Code').agg(agg).compute()


# %%
by_color.columns = ['price_mean', 'price_std', 'count']
by_color = by_color.reset_index()
# %%
pn.ggplot(by_color, pn.aes('Vehicle Color', 'count')) + pn.geom_col() + pn.coord_flip()
# %%
(
  pn.ggplot(by_color, pn.aes('Vehicle Color', 'price_mean')) 
  + pn.geom_col()
  + pn.geom_errorbar(pn.aes(ymin='price_mean - price_std', ymax='price_mean + price_std'), width=.2)
  + pn.coord_flip()
)


# %%
by_body_type.columns = ['price_mean', 'price_std', 'count']
by_body_type = by_body_type[by_body_type['count'] > 10000].reset_index()
# %%
pn.ggplot(by_body_type, pn.aes('Vehicle Body Type', 'count')) + pn.geom_col() + pn.coord_flip()
# %%
(
  pn.ggplot(by_body_type, pn.aes('Vehicle Body Type', 'price_mean')) 
  + pn.geom_col()
  + pn.geom_errorbar(pn.aes(ymin='price_mean - price_std', ymax='price_mean + price_std'), width=.2)
  + pn.coord_flip()
)


# %%
by_make.columns = ['price_mean', 'price_std', 'count']
by_make = by_make[by_make['count'] > 10000].reset_index()
# %%
pn.ggplot(by_make, pn.aes('Vehicle Make', 'count')) + pn.geom_col() + pn.coord_flip()
# %%
(
  pn.ggplot(by_make, pn.aes('Vehicle Make', 'price_mean')) 
  + pn.geom_col()
  + pn.geom_errorbar(pn.aes(ymin='price_mean - price_std', ymax='price_mean + price_std'), width=.2)
  + pn.coord_flip()
)



# %%
a = pl.from_pandas(by_vehicle_year.reset_index())
a.columns = ['vehicle_year', 'price_mean', 'price_std', 'count']
by_vehicle_year = (a
  .filter(pl.col('vehicle_year') > 1970)
  .filter(pl.col('vehicle_year') < 2022)
  .to_pandas()
)
# %%
pn.ggplot(by_vehicle_year, pn.aes('vehicle_year', 'count')) + pn.geom_col()
# %%
(
  pn.ggplot(by_vehicle_year, pn.aes('vehicle_year', 'price_mean')) 
  + pn.geom_col()
  + pn.geom_errorbar(pn.aes(ymin='price_mean - price_std', ymax='price_mean + price_std'), width=.2)
)


# %%
by_borough.columns = ['price_mean', 'price_std', 'count']
by_borough = by_borough.reset_index()
by_borough['Borough Code'] = by_borough['Borough Code'].astype(int)

borough_codes = pd.DataFrame([
  [1, 'Manhattan'],
  [2, 'The Bronx'],
  [3, 'Brooklyn'],
  [4, 'Queens'],
  [5, 'Staten Island'],
], columns = ['Borough Code', 'Borough'])

by_borough = by_borough.set_index('Borough Code').join(borough_codes.set_index('Borough Code'))

# %%

pn.ggplot(by_borough, pn.aes('Borough', 'count')) + pn.geom_col()
# %%

(
  pn.ggplot(by_borough, pn.aes('Borough', 'price_mean')) 
  + pn.geom_col()
  + pn.geom_errorbar(pn.aes(ymin='price_mean - price_std', ymax='price_mean + price_std'), width=.2)
)

