# Silver sellers: format location and flag sellers with recorded sales.
# Grain: 1 row per seller (seller_id).
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="olist.silver.sellers")
def silver_sellers():
    sellers = spark.read.table("sellers")
    # 1. Build a lookup of sellers with recorded sales (distinct seller_id from order_items)
    sales_per_seller = (
        spark.read.table("olist.silver.order_items")
        .select('seller_id')
        .distinct()  # One row per seller, ignoring repeats across orders and items
        .withColumn('has_sales', F.lit(True))  # Flag set to true for every seller found in order_items
    )

    return (
        sellers

        # 2. Left join sellers against the sales lookup
        .join(sales_per_seller, on='seller_id', how='left')

        # 3. Format location columns
        .withColumn('seller_zip_code_prefix', F.col("seller_zip_code_prefix").cast('string'))
        .withColumn('seller_city', F.trim(F.initcap('seller_city')))
        # 4. Sellers not found in the lookup become has_sales = false
        .withColumn('has_sales', F.coalesce(F.col('has_sales'), F.lit(False))
        )
    )
