import datetime
import unittest
from pyspark.sql import SparkSession, DataFrame

from src.retail.model.purchase import Purchase
from src.pyspark_structured_streaming_live.purchase_analytics import PurchaseAnalytics



class PurchaseAnalyticsTableTest(unittest.TestCase):
    
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
        print(f"  Web UI: {sc.uiWebUrl}")
        # You can get any specific property from the SparkConf object.
        conf = sc.getConf()
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
        
        df = self.spark.createDataFrame(records, schema=Purchase.schema)
        actual_df = PurchaseAnalytics.filter_purchases(df, upper_bound=upper_bound, lower_bound=lower_bound)
        actual_df.show()
        
