from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat, lit
from pyspark.sql.types import StringType
from pathlib import Path


PROJECT_PATH = Path(__file__).parent.parent.parent
DATA_PATH = PROJECT_PATH / 'data'
assert DATA_PATH.exists()

spark :SparkSession = SparkSession.builder    \
        .appName("hello-spark") \
        .master("local[*]")     \
        .getOrCreate()

print(spark.conf.get("spark.app.name"))
print(f"Spark version: {spark.version}")

csv_path = str(DATA_PATH / 'AAPL.csv')

df = spark.read.csv(csv_path)                                   # does not consider 1st row as header
df.printSchema()

df = spark.read.csv(csv_path, header=True)                      # considers header but all columns are strings
df.printSchema()

df = spark.read.csv(csv_path, header=True, inferSchema=True)    # considers header and scans csv to infer data types (costly)
df.printSchema()

df.show(5)


cast_date_expr = concat(                                        # create a concatenated column from the args
    col('Date')                                                 # retrieve column Date
    .cast(StringType()),                                        # and cast it to str
    lit(' hello world')                                         # create a literal column
)

df.withColumn('new_date', cast_date_expr).show(5)                # adds new column to the df and shows it



spark.stop()
