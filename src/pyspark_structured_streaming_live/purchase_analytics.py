from pyspark.sql import functions as F
from pyspark.sql import DataFrame


class PurchaseAnalytics:
    
    @staticmethod
    def filter_purchases(df: DataFrame, upper_bound:float, lower_bound:float) -> DataFrame:
        pass
    
    
    @staticmethod
    def product_total_spend(df: DataFrame) -> DataFrame:
        return (df
                .groupBy('productId')
                .agg(
                        F.sum(F.col('quantity') * F.col('pricePerUnit')).alias('productTotalSpend')
                    )
                )
    