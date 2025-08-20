import datetime
import shutil
import unittest
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import types as T

from src.retail.model.purchase import Purchase
from src.pyspark_structured_streaming_live.purchase_analytics import PurchaseAnalytics



class PurchaseAnalyticsTableTest(unittest.TestCase):
    
    # temp data path
    tmp_dir = f'/tmp/spark-streaming-test'
    # temp parquet table path
    table_path = f'{tmp_dir}/purchase-analytics-table-test'
    
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession for all tests."""
        print("starting spark session...")
        cls.spark: SparkSession = (
            SparkSession.builder.appName("purchase-analytics-test")
            .master("local[*]")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .getOrCreate()
        )
        # You can access the core properties directly from the SparkSession and its SparkContext.
        sc = cls.spark.sparkContext
        print("\n--- Spark Session Info ---")
        print(f"  Version: {cls.spark.version}")
        print(f"  Master: {sc.master}")
        print(f"  App Name: {sc.appName}")
        # The uiWebUrl property provides the link to the Spark UI.
        conf = cls.spark.conf
        print(f"  Driver Bind Address: {conf.get('spark.driver.bindAddress')}")
        print("--------------------------\n")


    @classmethod
    def tearDownClass(cls):
        """Stop SparkSession after all tests."""
        print("stopping spark session...")
        cls.spark.stop()
        print("spark session stopped")
        cls.remove_temp_files(cls.tmp_dir)
        print("temp files removed")
    
    
    @staticmethod
    def remove_temp_files(path:str):
        """Remove temp files."""
        shutil.rmtree(path, ignore_errors=True)

    
    def get_streaming_df(self, records: list[dict], schema: T.StructType) -> DataFrame:
        """creates a streaming df from records."""
        df = self.spark.createDataFrame(records, schema=schema)
        
        # write df to parquet
        (df
         .write
         .mode("overwrite")                         # deletes if existing and creates path and file
         .format("parquet")                         # output data format
         .save(self.table_path))                    # save to output path
        
        # read stream from parquet
        streaming_df = (self.spark.readStream
                        .format("parquet")          # source data format
                        .schema(schema)    # data chema
                        .load(self.table_path))     # stream from path
        
        return streaming_df
    
    
    def process_all_as_dicts(self, actual_streaming_df: DataFrame) -> tuple[list, DataFrame]:
        """creates list of dicts from streaming df."""
        # stream to memory table
        query = (actual_streaming_df
            .writeStream
            .format("memory")                       # write in memory table
            .queryName("filtered")                  # name of the in-memory table   
            .outputMode("append")                   # append mode
            .start())                               # start streaming  the query
        
        # get all available data
        query.processAllAvailable()
        
        # get final df
        actual_df = self.spark.sql("select * from filtered")
        actual_df.show()
        actual_dicts = [row.asDict() for row in actual_df.collect()]
        return actual_dicts, query

    
    def test_filter_purchases(self):
        upper_bound = 10.0
        lower_bound = 0.0
        
        # input test records
        records = [
            {
                "purchaseId": "id-1", "customerId": "c-1", "productId": "p-1", "quantity": 2, "pricePerUnit": 10.0, 
                "purchaseTimestamp": datetime.datetime.fromisoformat("2025-08-01T10:00") },
            {
                "purchaseId": "id-2", "customerId": "c-1", "productId": "p-2", "quantity": 3, "pricePerUnit": 1.0, 
                "purchaseTimestamp": datetime.datetime.fromisoformat("2025-08-01T11:00")
            },
        ]
        
        # expected test records
        expected = [
            {
                "purchaseId": "id-2", "customerId": "c-1", "productId": "p-2", "quantity": 3, "pricePerUnit": 1.0, 
                "purchaseTimestamp": datetime.datetime.fromisoformat("2025-08-01T11:00") 
            },
        ]
        
        streaming_df = self.get_streaming_df(records, Purchase.schema)
        
        # use the stream in the function
        actual_streaming_df = PurchaseAnalytics.filter_purchases(streaming_df, upper_bound=upper_bound, lower_bound=lower_bound)
        print(f'{actual_streaming_df.isStreaming = }')
        
        actual_dicts, query = self.process_all_as_dicts(actual_streaming_df)
        query.stop()
        
        
        self.assertEqual(len(expected), len(actual_dicts))
        self.assertIn(expected[0], actual_dicts)
