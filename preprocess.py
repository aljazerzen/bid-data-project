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

  df = pl.read_csv(path, rechunk=False, ignore_errors=True)

  df = df.with_columns([
    pl.col('Vehicle Body Type').cast(pl.Categorical),
    pl.col('Vehicle Make').cast(pl.Categorical),
    pl.col('Plate Type').cast(pl.Categorical),
    pl.col('Issuing Agency').cast(pl.Categorical),
    pl.col('Law Section').cast(pl.Utf8).cast(pl.Categorical),
  ])

  print('augment_address')
  df = augment_address(df)
  
  print('augment_cafes')
  df = augment_cafes(df)

  print('augment_violation')
  df = augment_violation(df)
  
  print('augment_car_color')
  df = augment_car_color(df)

  print('augment_ticket_datetime')
  df = augment_ticket_datetime(df)
  
  df = df.select([
    'Vehicle Body Type',
    'Vehicle Make',
    'Vehicle Year',
    'zipcode',
    'Borough Code',
    'NEW Street name',
    'BOROCODE',
    'lng',
    'lat',
    'cafe_count',
    'Violation Price',
    'Vehicle Color',
    'time_of_ticket',
  ])
  df = df.rename({'NEW Street name' : 'Street name'})

  print('augment_school')
  df = augment_school(df)

  print(df.head())

  out_file = f'./data/{filename}.gz.parquet'
  df.write_parquet(out_file, compression="gzip")

  print(f'done: {out_file} {os.stat(out_file).st_size / 1024 / 1024:.2f}MB')

if __name__ == "__main__":
  preprocess()