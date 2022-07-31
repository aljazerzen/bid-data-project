import polars as pl
import pandas as pd

def augment_ticket_datetime(df: pl.DataFrame, yyear):
	if yyear==2020:
		out = df.with_columns(
			[
				pl.concat_str(["Issue Date", pl.lit(" ") ,  "Violation Time", pl.lit("M")])
				.str.strptime(pl.Datetime, "%m/%d/%Y 12:00:00 AM %I%M%p", strict=False).alias("time_of_ticket")
			]
		).drop(['Issue Date','Violation Time'])
		
	else:
		out = df.with_columns(
			[
				pl.concat_str(["Issue Date", pl.lit(" ") ,  "Violation Time", pl.lit("M")])
				.str.strptime(pl.Datetime, "%m/%d/%Y %I%M%p", strict=False).alias("time_of_ticket")
			]
		).drop(['Issue Date','Violation Time'])

	# Set year
	tmp = pd.DataFrame(out).transpose()
	tmp.columns = out.columns
	tmp = tmp.dropna(subset=['time_of_ticket'])
	tmp['time_of_ticket'] = tmp['time_of_ticket'].apply(lambda x: x.replace(year = yyear))
	out = pl.DataFrame(tmp)
	
	return out