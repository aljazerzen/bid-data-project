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
  + pn.geom_point(pn.aes("lng", "lat"), alpha=0.1, size=0.04, color="blue")
  # + pn.stat_density_2d(pn.aes("lng", "lat", alpha=pn.after_stat('density')), geom='tile', contour=False)
  + pn.coord_fixed()
  + pn.theme(axis_title_x=pn.element_blank(), axis_title_y=pn.element_blank())
)
p.save('./report/figures/nyc.map.png', width=10, height=10)
p
