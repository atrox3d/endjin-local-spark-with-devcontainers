from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, year, row_number
from pyspark.sql.types import StringType
from pathlib import Path


PROJECT_PATH = Path(__file__).parent.parent.parent
DATA_PATH = PROJECT_PATH / 'data'
assert DATA_PATH.exists()



spark :SparkSession = SparkSession.builder    \
        .appName("load-csv") \
        .master("local[*]")     \
        .getOrCreate()

print(f"Spark version: {spark.version}")
csv_path = str(DATA_PATH / 'AAPL.csv')

# Read the data, inferring the schema for correct data types.
df = spark.read.csv(
    csv_path,
    header=True, 
    inferSchema=True
)
    # OR
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(csv_path)


str_year = (
    df['Date']                          # get Column obj reference
    .cast(StringType())                 # cast to string Column
    .substr(1, 4)                       # extract the year as substring as Column
    .alias('YEAR')                      # rename the column (returns Column obj)
)
df.select(
    df['Date'],                         # get Column ref: or 'Date' or col('Date')
    str_year                            # use the year substr column in select
).show(5)


spark.stop()
