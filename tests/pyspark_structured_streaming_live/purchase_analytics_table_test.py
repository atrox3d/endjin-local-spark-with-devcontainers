import datetime
import unittest
from pyspark.sql import SparkSession, DataFrame
from pathlib import Path

from src.retail.model.purchase import Purchase
from src.pyspark_structured_streaming_live.purchase_analytics import PurchaseAnalytics



class PurchaseAnalyticsTableTest(unittest.TestCase):
    # input test records
    records = [
        {
            "purchaseId": "id-1",
            "customerId": "c-1",
            "productId": "p-1",
            "quantity": 2,
            "pricePerUnit": 10.0,
            "purchaseTimestamp": datetime.datetime.fromisoformat("2025-08-01T10:00")
        },
        {
            "purchaseId": "id-2",
            "customerId": "c-1",
            "productId": "p-2",
            "quantity": 3,
            "pricePerUnit": 1.0,
            "purchaseTimestamp": datetime.datetime.fromisoformat("2025-08-01T11:00")
        },
    ]
    
    # expected test records
    expected = [
        {
            "purchaseId": "id-2",
            "customerId": "c-1",
            "productId": "p-2",
            "quantity": 3,
            "pricePerUnit": 1.0,
            "purchaseTimestamp": datetime.datetime.fromisoformat("2025-08-01T11:00")
        },
    ]
    
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
    
    
    def test_filter_purchases(self):
        upper_bound = 10.0
        lower_bound = 0.0
        
        df = self.spark.createDataFrame(self.records, schema=Purchase.schema)
        
        # write df to parquet
        (df
         .write
         .mode("overwrite")        # deletes if existing and creates path and file
         .format("parquet")
         .save(self.table_path))
        
        # read stream from parquet
        streaming_df = (self.spark.readStream
                        .format("parquet")
                        .schema(Purchase.schema)
                        .load(self.table_path))
        
        # use the stream in the function
        actual_streaming_df = PurchaseAnalytics.filter_purchases(streaming_df, upper_bound=upper_bound, lower_bound=lower_bound)
        print(f'{actual_streaming_df.isStreaming = }')
        
        # stream to memory table
        query = (actual_streaming_df.writeStream
            .format("memory")
            .queryName("filtered")
            .outputMode("append")
            .start())
        
        # terminate streaming
        query.processAllAvailable()
        
        # get final df
        actual_df = self.spark.sql("select * from filtered")
        actual_df.show()
        