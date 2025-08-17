from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, year, month, row_number, date_add, lit, concat, max, avg, sum
from pyspark.sql.types import StringType, DateType, DoubleType
from pathlib import Path


PROJECT_PATH = Path(__file__).parent.parent.parent
DATA_PATH = PROJECT_PATH / 'data'
assert DATA_PATH.exists()



spark :SparkSession = SparkSession.builder    \
        .appName("load-csv") \
        .master("local[*]")     \
        .getOrCreate()

print(f"Spark version: {spark.version}")


def load_stock_data(symbol:str, datapath:str|Path=DATA_PATH) -> DataFrame:
    csv_path = f'{datapath!s}/{symbol}.csv'
    print(f'loading {csv_path}...')

    # Read the data, not inferring the schema for correct data types.
    df = spark.read.csv(
        csv_path,
        header=True, 
        # inferSchema=True
    )
    
    # specify schema instead of inferring it
    # and rename columns
    return df.select(
        df['Date'].cast(DateType()).alias('date'),
        df['Open'].cast(DoubleType()).alias('open'),
        df['Close'].cast(DoubleType()).alias('close'),
        df['High'].cast(DoubleType()).alias('high'),
        df['Low'].cast(DoubleType()).alias('low'),
    )

df = load_stock_data('AAPL')

print('max close by year and month:')
(df.groupBy(
        year(df['date']),
        month(df['date']),
    ).max('close')
).show(10)
print()

print('sort by date before max close by year and month:')
(df.sort('date')                                                    # no effect on groupby
    .groupBy(
        year(df['date']),
        month(df['date']),
    ).max('close')
).show(10)
print()

print('sort by year, month after max close by year and month:')
(df.groupBy(
        year(df['date']),
        month(df['date']),
    )
    .max('close')
    # .sort('year(date)', 'month(date)')
    .sort(year(df['date']), month(df['date']))                      # correct
    
).show(10)
print()

print('sort by year, month after max close by year and month:')
(df.groupBy(
        year(df['date']),
        month(df['date']),
    )
    .agg(                                                           # multiple aggregations on same groups
        max('close').alias('max_close'),
        avg('close').alias('avg_close'),
        sum('open').alias('sum_open'),
    )
    .sort(
        # 'max_close',                                              # could raise error of non existing column
        # ascending=False                                           # does not work with col().desc()
        col('max_close').desc(),
    )
).show(10)
print()


spark.stop()
