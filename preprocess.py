import polars as pl
import os

from address_augmentation import address_augment

def preprocess(path = './input/2022.csv'):

  filename = os.path.basename(path)
  filename = filename[:filename.rfind('.')]

  df = pl.read_csv(path, rechunk=False, ignore_errors=True, )

  df = df.with_columns([
    pl.col('Vehicle Body Type').cast(pl.Categorical),
    pl.col('Vehicle Make').cast(pl.Categorical),
    pl.col('Plate Type').cast(pl.Categorical),
    pl.col('Issuing Agency').cast(pl.Categorical),
    pl.col('Law Section').cast(pl.Utf8).cast(pl.Categorical),
  ])

  df = address_augment(df)

  print(df.head())

  # TODO select only columns we need

  out_file = f'./data/{filename}.gz.parquet'
  df.write_parquet(out_file, compression="gzip")

  size = os.stat(out_file).st_size
  print(f'done. out size: {size / 1024 / 1024:.2f}')

if __name__ == "__main__":
  preprocess()