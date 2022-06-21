# %%
import polars as pl

def augment_address(df: pl.DataFrame):
  # %%
  bobaadr = pl.read_parquet('./data/bobaadr.parquet')

  # %%
  bobaadr = (bobaadr
    .unique(subset=['sc5', 'hhnd'])
    .select([
      pl.col('sc5').alias('Street Code'),
      pl.col('scboro').cast(pl.Utf8).cast(pl.Categorical).alias('Borough Code'),
      pl.col('hhnd').str.lstrip().alias('House Number'),
      pl.col('bin').alias('BIN'),
      pl.col('physical_id').cast(pl.Int64, strict=False).alias('PHYSICALID'),
      pl.col('zipcode'),
    ])
  )
  bobaadr.head(1000).tail()

  # %% add BIN, PHYSICALID and Borough Code and ZIP code
  df = (df
    .with_column(pl.col('House Number').fill_null(''))
    .join(bobaadr.select(['Street Code', 'House Number', 'BIN', 'PHYSICALID', 'zipcode']), left_on=['Street Code1', 'House Number'], right_on=['Street Code', 'House Number'], how='left')
    .join(bobaadr.select(['Street Code', 'Borough Code']).unique(subset='Street Code'), left_on=['Street Code1'], right_on=['Street Code'], how='left')
    .drop(['Street Code1', 'House Number'])
  )


  # %%
  ap = pl.read_parquet('data/address_point.parquet', ignore_errors=True)

  # %%
  ap = (ap
    .with_columns([
      pl.col('the_geom').str.slice(7).str.replace(' .*', '').cast(pl.Float64).alias('lng'),
      pl.col('the_geom').str.replace('.* ', '').str.replace('\)', '').cast(pl.Float64).alias('lat'),
    ])
    .select(['BIN', 'PHYSICALID', 'FULL_STREE', 'BOROCODE', 'lng', 'lat'])
  )

  # %%
  df = (df
    .join(ap, on=['BIN', 'PHYSICALID'])
    .drop(['BIN', 'PHYSICALID'])
  )

  return df.rename({"FULL_STREE": "NEW Street name"})


