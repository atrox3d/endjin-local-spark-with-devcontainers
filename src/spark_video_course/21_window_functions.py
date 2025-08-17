from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, year, month, row_number, date_add, lit, concat, max, avg, sum, rank
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
# simplest window: all rows ordered by one column (tie column)
#####################################################################################
window = (Window
    .partitionBy()              # create a window for all rows
    .orderBy(df['date'])        # expected ordering, throws error otherwise
)

print('[SINGLE WINDOW ALL RECORDS SORTED BY DATE]')
df.withColumn(                  # add a column for the window function
    'row_number',               # column name
    row_number().over(window)   # window function
).show()

#####################################################################################
# split data into multiple windows, one for each year
#####################################################################################
window = (Window
    .partitionBy(year(df['date']))      # create a window for each year
    .orderBy(df['close'].desc())        # sort by close price descending within each year window
)

print('[ONE WINDOW FOR EACH YEAR, SORTED BY CLOSING PRICE DESCENDING]')
df.withColumn('row_number', row_number().over(window)).show()

print('[ONE WINDOW FOR EACH YEAR, SORTED BY CLOSING PRICE DESCENDING: FILTER 1ST ROW OF EACH WINDOW]')
df.withColumn(
    'row_number', row_number().over(window)
).filter(
    col('row_number') == 1
).show(5)

#####################################################################################
# split data into multiple windows, one for each year
#####################################################################################
window = (Window
    .partitionBy(year(df['date']))      # create a window for each year
    .orderBy(
        df['close'].desc(),             # sort by close price descending within each year window
        df['date'])                     # this has no effect
)

print('[ONE WINDOW FOR EACH YEAR, SORTED BY CLOSING PRICE DESCENDING, DATE ASCENDING]')
df.withColumn('row_number', row_number().over(window)).show()

print('[ONE WINDOW FOR EACH YEAR, SORTED BY CLOSING PRICE DESCENDING, DATE ASCENDING: FILTER 1ST ROW OF EACH WINDOW]')
df.withColumn(
    'row_number', row_number().over(window)
).filter(
    col('row_number') == 1              # get the highest (1st row) close value
).show(5)


spark.stop()
