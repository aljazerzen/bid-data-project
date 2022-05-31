# %%
import polars as pl

def augment_cafes(df: pl.DataFrame):

  # %%
  cf = pl.read_csv('data/Sidewalk_Cafe__Licenses.csv', ignore_errors=True)
  cf.head()

  # %%
  cf = cf.groupby('ZIP').count().select([
    pl.col('ZIP').cast(pl.Utf8).alias('zipcode'),
    pl.col('count').alias('cafe_count'),
  ])
  
  return df.join(cf, on='zipcode')

if __name__ == '__main__':

  df = pl.scan_parquet('data/2022.gz.parquet').head(100).fetch()

  df = augment_cafes(df)
  print(df.head())
