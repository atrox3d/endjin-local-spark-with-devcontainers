from pyspark.sql import SparkSession
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

spark.stop()
