from pyspark.sql import functions as F
from pyspark.sql import DataFrame


class PurchaseAnalytics:
    
    @staticmethod
    def filter_purchases(df: DataFrame, upper_bound:float=10.0, lower_bound:float=1.0) -> DataFrame:
        return df
    
    
    @staticmethod
    def product_total_spend(df: DataFrame) -> DataFrame:
        return (df
                .groupBy('productId')
                .agg(
                        F.sum(F.col('quantity') * F.col('pricePerUnit')).alias('productTotalSpend')
                    )
                )
    