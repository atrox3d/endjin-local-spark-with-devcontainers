from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, concat, lit, year, row_number
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


# this should be the corresponding SQL
# SELECT *,
# ROW_NUMBER() OVER(PARTITION BY year(Date) ORDER BY Close desc) rank,
# FROM csv
# WHERE rank = 1

window = Window.partitionBy(                                    # create sql windows function
    year(col('Date'))                                           # partitioning by the year of the Date
).orderBy(
    col('Close').desc()                                         # order window rows by Close DESC
)

df.withColumn(                                                  # add rank column
    'rank', 
    row_number().over(window)                                   # numbering the window records
).filter(                                                       # filter only first records in windows
    col('rank') == 1
).drop(                                                         # drop rank column
    'rank'
).show()



spark.stop()
