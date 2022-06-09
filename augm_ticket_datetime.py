import polars as pl

def augment_ticket_datetime(df: pl.DataFrame):
	out = df.with_columns(
		[
			pl.concat_str(["Issue Date", pl.lit(" ") ,  "Violation Time", pl.lit("M")])
			.str.strptime(pl.Datetime, "%m/%d/%Y %I%M%p", strict=False).alias("time_of_ticket")
		]
	).drop(['Issue Date','Violation Time'])
	
	return out