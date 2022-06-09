import polars as pl
import os

from augm_address import augment_address
from augm_cafe import augment_cafes
from augm_violation import augment_violation
from augm_car_color import augment_car_color
from augm_ticket_datetime import augment_ticket_datetime
from augm_school import augment_school

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

  df = augment_address(df)
  
  df = augment_cafes(df)

  df = augment_violation(df)
  
  df = augment_car_color(df)

  df = augment_ticket_datetime(df)
  
  df = augment_school(df)

  print(df.head())

  # TODO select only columns we need

  out_file = f'./data/{filename}.gz.parquet'
  df.write_parquet(out_file, compression="gzip")

  print(f'done: {out_file} {os.stat(out_file).st_size / 1024 / 1024:.2f}MB')

if __name__ == "__main__":
  preprocess()