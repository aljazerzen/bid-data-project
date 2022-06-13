# %%

import polars as pl
import geopandas as gp
import plotnine as pn
import matplotlib.pyplot as plt

# %%

df = pl.read_parquet('./data/2022.gz.parquet')

# %%

boroughs = gp.read_file('data/nyc_boroughs.shx')
boroughs
# %%
pick = [i != 2 for i in range(len(boroughs.geometry))]
boroughs = boroughs[pick]
# %%

p = (pn.ggplot(df.head(100000).to_pandas())
  + pn.geom_map(boroughs, fill='gray', color=None)
  + pn.geom_point(pn.aes("lng", "lat"), alpha=0.05, size=0.02)
  # + pn.stat_density_2d(pn.aes("lng", "lat", alpha=pn.after_stat('density')), geom='tile', contour=False)
  + pn.coord_fixed()
)
p.save('figures/nyc.map.pdf')
p
