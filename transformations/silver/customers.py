# Silver customers: location standardization, region mapping and customer-level flags.
# Grain: 1 row per order (customer_id x order_id).
from pyspark import pipelines as dp
from pyspark.sql import functions as F, window as W


@dp.materialized_view(name="olist.silver.customers")
def silver_customers():
    customers = spark.read.table("customers")
    # 1. Window partitioned by customer_unique_id for customer-level flags (grain stays 1 row per order)
    w1 = W.Window.partitionBy("customer_unique_id")
    return (
        customers
        .join(spark.read.table("olist.silver.orders").select("customer_id", "order_id", "order_status"), on='customer_id')
        # 2. Cast zip code prefix to int
        .withColumn('customer_zip_code_prefix', F.col("customer_zip_code_prefix").cast('int'))
        # 3. Standardize city and state formatting
        .withColumn('customer_city', F.initcap("customer_city"))
        .withColumn("customer_state", F.upper("customer_state"))
        # 4. Map each state to its region
        .withColumn('customer_region',
                    F.when(F.col("customer_state").isin("AC", "AP", "AM", "PA", "RO", "RR", "TO"), "north")
                    .when(F.col("customer_state").isin("MA", "PI", "CE", "RN", "AL", "BA", "SE", "PE"), "north-east")
                    .when(F.col("customer_state").isin("MT", "MS", "GO", "DF"), "mid-west")
                    .when(F.col("customer_state").isin("PR", "SC", "RS"), "south")
                    .when(F.col("customer_state").isin("MG", "ES", "RJ", "SP"), "south-east"))
        # 5. Flag: customer with divergent locations across orders
        .withColumn("location_inconsistent",
            (F.size(F.collect_set("customer_city").over(w1)) > 1) |
            (F.size(F.collect_set("customer_state").over(w1)) > 1))
        # 6. Flag: repeat customer (placed more than one order)
        .withColumn('is_repeat_customer',
                    F.when(F.size(F.collect_set('order_id').over(w1)) > 1, True).otherwise(False))
        # 7. Flag: customer has canceled at least one order
        .withColumn('has_canceled_order',
            F.when(F.array_contains(F.collect_set('order_status').over(w1), 'canceled'), True).otherwise(False))
    )
