import polars as pl


def augment_car_color(df: pl.DataFrame):
	dfcar = pl.read_csv('data/car_color.csv')

	dfcar = dfcar.select([
		'Original',
		pl.col('New').alias('New Color')
	])

	return df.join(dfcar, left_on = 'Vehicle Color', right_on='Original').drop('Vehicle Color').rename({'New Color':'Vehicle Color'})
  