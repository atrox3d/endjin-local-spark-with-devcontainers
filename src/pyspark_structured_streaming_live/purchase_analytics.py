from pyspark.sql import functions as F
from pyspark.sql import DataFrame


class PurchaseAnalytics:
    
    @staticmethod
    def product_aggregations(df: DataFrame) -> DataFrame:
        result =  (df
                .groupBy('productId')
                .agg(
                    F.sum(F.col('quantity') * F.col('pricePerUnit')).alias('totalOrderVolume'),
                    F.max('purchaseTimestamp').alias('latestPurchaseTimestamp')
                )
        )
        
        return result
    
    @staticmethod
    def filter_purchases(df: DataFrame, upper_bound:float=10.0, lower_bound:float=1.0) -> DataFrame:
        total_spend = F.col('quantity') * F.col('pricePerUnit')
        filter_cond = (total_spend <= upper_bound) & (total_spend >= lower_bound)
        return df.filter(filter_cond)
    
    @staticmethod
    def product_total_spend(df: DataFrame) -> DataFrame:
        return (df
                .groupBy('productId')
                .agg(
                        F.sum(F.col('quantity') * F.col('pricePerUnit')).alias('productTotalSpend')
                    )
                )
    