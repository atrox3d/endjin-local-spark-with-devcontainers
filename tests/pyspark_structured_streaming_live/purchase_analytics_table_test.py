import unittest
from pyspark.sql import SparkSession, DataFrame

from src.retail.model.purchase import Purchase
from src.pyspark_structured_streaming_live.purchase_analytics import PurchaseAnalytics



class PurchaseAnalyticsTableTest(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        """Set up SparkSession for all tests."""
        cls.spark = (
            SparkSession.builder.appName("hcp-test")
            .master("local[*]")
            .getOrCreate()
        )


    @classmethod
    def tearDownClass(cls):
        """Stop SparkSession after all tests."""
        cls.spark.stop()

    
    def test_filter_purchases(self):
        pass
