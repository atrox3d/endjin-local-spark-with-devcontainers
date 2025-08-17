from pyspark.sql import SparkSession, Window, DataFrame
from pyspark.sql.functions import col, year, row_number, date_add, lit, concat
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

date_plus_2_days = date_add(df['date'], 2)
date_as_string = date_plus_2_days.cast(StringType())
concat_column = concat(
    date_as_string,
    lit('-hello world')
)

df.show(5)

(df.sort(
        'date',                     # column by str name
        ascending=False             # kwargs
    )
).show(5)

(df.sort(
        df['date'].desc()           # Colum obj
    )
).show(5)

(df.sort(
        date_add(
            df['date'], 2               # transofrm column
        ).desc()                        # sort order
    )
).show(5)

(df.sort(                               # multiple rows
    df['date']                          
    .cast(StringType()).substr(1, 7)    # get year-month, descending
    .desc(),                            
    df['close'])                        # close increases inside the yyyy-mm range
).show(50)

df = spark.createDataFrame(
    [(x, y) for x in range(5) for y in range(5)], 
    schema=['x', 'y']
)

df.show()
df.sort(
    df['x'].desc(),
    df['y']
).show()

spark.stop()
