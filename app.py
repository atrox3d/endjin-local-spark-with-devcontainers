from pyspark.sql import SparkSession  # Import the main entry point for DataFrame and SQL functionality.

def main():
    """Main function to run the Spark job."""
    # Initialize Spark Session
    spark = (                                           # Start building a new SparkSession.
        SparkSession.builder                            # Get the SparkSession builder.
        .appName("HelloWorld")                          # Set a name for the application, which will appear in the Spark UI.
        .master("local[*]")                             # Set the Spark master URL. "local[*]" means run locally using all available CPU cores.
        .getOrCreate()                                  # Get an existing SparkSession or, if there is none, create a new one.
    )

    print(f"Spark version: {spark.version}")            # Print the version of the running Spark instance.

    # Create a simple DataFrame
    data = [("Alice", 1), ("Bob", 2), ("Charlie", 3)]   # Define the data as a list of tuples. Each tuple is a row.
    columns = ["name", "id"]                            # Define the column names for the DataFrame.
    df = spark.createDataFrame(data, columns)           # Create a Spark DataFrame from the data and column names.

    # Show the DataFrame
    print("Here is a sample DataFrame:")                # Print a descriptive message to the console.
    df.show()                                           # Display the content of the DataFrame in a table format. This is a Spark "action".

    # Stop the Spark session
    spark.stop()                                        # Gracefully stop the SparkSession to release its resources.

if __name__ == "__main__":                              # Standard Python entry point.
    main()                                              # Call the main function to execute the script.
