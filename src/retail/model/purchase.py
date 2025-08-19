from pyspark.sql import types as T


class Purchase:
    
    schema: T.StructType = T.StructType([
        T.StructField('purchaseId',         T.StringType(),     nullable=False),
        T.StructField('customerId',         T.StringType(),     nullable=False),
        T.StructField('productId',          T.StringType(),     nullable=False),
        T.StructField('quantity',           T.IntegerType(),    nullable=False),
        T.StructField('pricePerUnit',       T.DoubleType(),     nullable=False),
        T.StructField('purchaseTimestamp',  T.TimestampType(),  nullable=False),
    ])
