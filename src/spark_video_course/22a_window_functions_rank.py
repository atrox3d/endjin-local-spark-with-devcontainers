from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, year, row_number, rank, dense_rank
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

################################################################################################
# Demonstrating row_number vs rank vs dense_rank
#
# The key difference is how they handle ties in the ordering.
# - row_number(): Assigns a unique number (1, 2, 3, 4). Non-deterministic for ties.
# - rank(): Assigns the same rank for ties, but leaves gaps (1, 1, 3, 4).
# - dense_rank(): Assigns the same rank for ties, but does not leave gaps (1, 1, 2, 3).
################################################################################################

# We will partition the data by year and order by the closing price in descending order.
# This will rank the days within each year based on their closing price.
price_rank_window = (Window
    .partitionBy(year(df['date']))
    .orderBy(df['close'].desc())        # tie value
)

(df
    # .filter(year(df['date']) == 2018)                               # Focus on a single year
    .withColumn('row_number', row_number().over(price_rank_window))
    .withColumn('rank', rank().over(price_rank_window))
    .withColumn('dense_rank', dense_rank().over(price_rank_window))
    # .filter(col('dense_rank') <= 3)                                 # Show the top 3 price ranks
    .sort('close', ascending=False)
).show()

spark.stop()
