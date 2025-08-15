from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import col, year, row_number
from pathlib import Path


PROJECT_PATH = Path(__file__).parent.parent.parent
DATA_PATH = PROJECT_PATH / 'data'
assert DATA_PATH.exists()

def main():
    """Main function to run the Spark job."""
    spark :SparkSession = SparkSession.builder    \
            .appName("hello-spark") \
            .master("local[*]")     \
            .getOrCreate()

    try:
        print(f"Spark version: {spark.version}")
        csv_path = str(DATA_PATH / 'AAPL.csv')

        # Read the data, inferring the schema for correct data types.
        df = spark.read.csv(csv_path, header=True, inferSchema=True)
        print("Original DataFrame schema:")
        df.printSchema()
        df.show(5)

        # --- Example of a window function to find the day with the highest closing price each year ---
        print("\nDay with the highest closing price per year:")
        window = Window.partitionBy(                                    # Create a window partitioned by year
            year(col('Date'))
        ).orderBy(
            col('Close').desc()                                         # Order within each year by closing price
        )

        (df.withColumn(                                                 # Add a 'rank' column
            'rank',
            row_number().over(window)                                   # Number rows within each window (year)
        ).filter(                                                       # Keep only the top-ranked row per year
            col('rank') == 1
        ).drop(                                                         # Remove the temporary rank column
            'rank'
        ).select(                                                       # Select and rename columns for the final output
            year(col('Date')).alias('Year'),                            # Explicitly use col() and alias the new column
            col('Date'),                                                # Use col() for consistency
            col('Close')                                                # Use col() for consistency
        ).show())

    finally:
        print("Stopping Spark session.")
        spark.stop()

if __name__ == "__main__":
    main()
