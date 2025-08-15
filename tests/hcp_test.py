import datetime
import unittest
from pyspark.sql import SparkSession, DataFrame
from pyspark.testing.utils import assertDataFrameEqual    # raises pandas required

from src.pypspark_live_coding.hello_world import highest_closing_price_per_year


class TestHighestClosingPricePerYear(unittest.TestCase):
    
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

    def test_without_ties(self):
        """
        Test the highest_closing_price_per_year function with sample data.
        """
        # Sample data with column names matching the function's expectations
        test_data = [
            {
                "date": datetime.date(year=2023, month=1, day=23),
                "close": 2.0,
                "open": 1.0
            },
            {
                "date": datetime.date(year=2023, month=1, day=24),
                "close": 1.0,
                "open": 2.0
            },
        ]
        test_df = self.spark.createDataFrame(test_data)

        # Expected result (without the intermediate 'rank' column)
        expected_data = [
            {
                "date": datetime.date(year=2023, month=1, day=23),
                "close": 2.0,
                "open": 1.0
            },
        ]
        expected_df = self.spark.createDataFrame(expected_data)
        expected_df.show()
        
        # Apply the function
        # this works because spark case sensitivity is off
        result_df = highest_closing_price_per_year(test_df).drop('rank')
        result_df.show()
        
        # Assert that the resulting DataFrame matches the expected one
        assertDataFrameEqual(result_df, expected_df)
