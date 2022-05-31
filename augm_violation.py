import polars as pl


def augment_violation(df: pl.DataFrame):
  dfticket = pl.read_csv('data/ticket_code_to_fine.csv')

  dfticket = dfticket.select([
    'VIOLATION CODE',
    pl.col('OTHER STREETS').alias('Violation Price'),
  ])

  return df.join(dfticket, left_on = 'Violation Code', right_on='VIOLATION CODE')
  