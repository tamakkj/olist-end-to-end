# Silver orders: cast timestamps and add delivery/consistency quality flags.
# Grain: 1 row per order (order_id).
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="olist.silver.orders")
def silver_orders():
    orders = spark.read.table("orders")
    return (
        orders
        # 1. Cast order timestamp columns from STRING to TIMESTAMP
        .withColumn("order_purchase_timestamp", F.to_timestamp("order_purchase_timestamp"))
        .withColumn("order_approved_at", F.to_timestamp("order_approved_at"))
        .withColumn("order_delivered_carrier_date", F.to_timestamp("order_delivered_carrier_date"))
        .withColumn("order_delivered_customer_date", F.to_timestamp("order_delivered_customer_date"))
        .withColumn("order_estimated_delivery_date", F.to_timestamp("order_estimated_delivery_date"))

        # 2. Flag: delivered order with incomplete delivery data
        .withColumn("order_data_incomplete",
            F.when(
                (F.col("order_status") == "delivered") & (
                    F.col("order_delivered_carrier_date").isNull() |
                    F.col("order_delivered_customer_date").isNull() |
                    F.col("order_estimated_delivery_date").isNull() |
                    F.col("order_purchase_timestamp").isNull() |
                    F.col("order_approved_at").isNull()
                    ), True).otherwise(False))

        # 3. Flag: delivered to customer before carrier handoff (temporal inconsistency)
        .withColumn("delivery_order_invalid",
            F.when(
                F.col("order_delivered_customer_date").isNotNull() &
                F.col("order_delivered_carrier_date").isNotNull() &
                (F.col("order_delivered_customer_date") < F.col("order_delivered_carrier_date")
                ), True).otherwise(False))

        # 4. Flag: late delivery (delivered after the estimated date)
        .withColumn("is_late_delivery",
            F.when(
                (F.col("order_status") == "delivered") &
                F.col("order_delivered_customer_date").isNotNull() &
                F.col("order_estimated_delivery_date").isNotNull() &
                (F.col("order_delivered_customer_date") > F.col("order_estimated_delivery_date")
                ), True).otherwise(False))

        # 5. Flag: order without approval (status is neither created nor canceled)
        .withColumn("missing_approval",
            F.when(
                F.col("order_approved_at").isNull() &
                ~F.col("order_status").isin("created", "canceled"),
                True).otherwise(False))
    )
