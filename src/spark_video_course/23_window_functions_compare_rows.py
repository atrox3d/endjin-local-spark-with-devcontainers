from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, when, lag
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

    print(f'loading {csv_path}...', end='')
    # Read the data, not inferring the schema for correct data types.
    df = spark.read.csv(
        csv_path,
        header=True, 
        # inferSchema=True
    )
    print(f'done.')
    
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

#####################################################################################
# Using lag() to compare a row with the previous row
#####################################################################################
window = Window.partitionBy().orderBy(col('date'))       # one window for the whole DataFrame, sorted by date

print('[Calculating difference between open and previous day close]')
(df
    .withColumn(
        'previousDayClose', 
        lag(
            col('close'),        # The column to look back on
            1,                   # The number of rows to look back (1 means the previous row)
            0.0                  # The default value if no previous row exists (i.e., for the first row)
        )
        .over(window)
    )
    .withColumn(
        'diff',
        # Use when() for conditional logic in Spark.
        # A Column object cannot be used in a Python `if` statement.
        when(
            col('previousDayClose') != 0.0,         # condition
            col('open') - col('previousDayClose')   # if true
        ).otherwise(None)                           # if false
    )
    .select(                                        # select only relevant rows
        col('date'), 
        col('open'), 
        col('close'), 
        col('previousDayClose'), 
        col('diff'))
    .show()
)

spark.stop()
