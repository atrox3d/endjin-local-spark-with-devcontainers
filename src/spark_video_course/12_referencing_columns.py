from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, year, row_number
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

col1 = df.Close                 # pandas lile column ref
col2 = df['Close']              # pansas like column ref
col3 = col('Close')             # generic unbound column ref


df.select(col1, col2, col3).show(5)
    # OR
df.select('Date', 'Close', 'Open').show(5)
spark.stop()
