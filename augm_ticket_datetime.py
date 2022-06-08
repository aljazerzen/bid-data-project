import polars as pl

def augment_ticket_datetime(df: pl.DataFrame):
	out = df.with_columns(
		[
			pl.concat_str(["date", pl.lit(" ") ,  "time", pl.lit("M")])
			.str.strptime(pl.Datetime, "%m/%d/%Y %I%M%p").alias("time_of_ticket")
		]
	).drop(['date','time'])
	
	return out