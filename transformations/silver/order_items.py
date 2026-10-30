# Silver order_items: cast types, round prices and add shipping quality flags.
# Grain: 1 row per order item (order_id x order_item_id).
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="olist.silver.order_items")
def silver_order_items():
    order_items = spark.read.table("order_items")
    return (
        order_items
        # 1. Join with silver orders for the purchase timestamp
        .join(spark.read.table("olist.silver.orders").select("order_id", "order_purchase_timestamp"), on="order_id")

        # 2. Cast types and round prices
        .withColumn("order_item_id", F.col("order_item_id").cast("int"))
        .withColumn("shipping_limit_date", F.to_timestamp("shipping_limit_date"))
        .withColumn("price", F.round(F.col("price"), 2))

        # 3. Flag: free shipping (zero freight)
        .withColumn("no_freight",
            F.when(F.col("freight_value") == 0, True).otherwise(False))
        # 4. Flag: shipping limit anomaly (limit more than 30 days after purchase, hour-aware)
        .withColumn("shipping_limit_anomaly",
            F.when(
                F.col("shipping_limit_date").isNotNull() &
                F.col("order_purchase_timestamp").isNotNull() &
                ((F.unix_timestamp(F.col("shipping_limit_date")) - F.unix_timestamp(F.col("order_purchase_timestamp"))) / 86400.0 > 30
            ), True).otherwise(False))
    )
