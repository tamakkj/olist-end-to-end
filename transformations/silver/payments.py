# Silver payments: cast types, round values and add payment quality flags.
# Grain: 1 row per payment (order_id x payment_sequential).
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="olist.silver.payments")
def silver_payments():
    payments = spark.read.table("payments")
    return (
        payments
        # 1. Left join with silver orders for status and timestamp context (LEFT keeps the has_order flag meaningful)
        .join(spark.read.table("olist.silver.orders").select("order_id", "order_status", "order_purchase_timestamp", "order_approved_at"), on='order_id', how='left')

        # 2. Cast types and round payment values
        .withColumn("payment_sequential", F.col("payment_sequential").cast("int"))
        .withColumn("payment_installments", F.col("payment_installments").cast("int"))
        .withColumn("payment_value", F.round(F.col("payment_value"), 2))

        # 3. Data quality flags
        .withColumn('defined_payment_type',
                    F.when(F.col('payment_type').isNotNull(), True).otherwise(False))
        .withColumn('is_zero_value',
                    F.when(F.col('payment_value') == 0, True).otherwise(False))
        .withColumn('inconsistent_payment',
                    F.when((((F.col('payment_type') != 'credit_card') & (F.col("payment_installments") > 1)) | ((F.col("payment_installments") == 0))), True).otherwise(False))
        .withColumn('has_order',
                    F.when(F.col('order_id').isNotNull(), True).otherwise(False))
    )
