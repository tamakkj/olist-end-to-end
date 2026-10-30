# 1. Supabase S3-compatible storage credentials, read from Databricks secrets.
#    Create the scope and secrets once (CLI or workspace UI) — never hardcode keys here:
#      databricks secrets create-scope olist
#      databricks secrets put-secret olist aws_access_key_id
#      databricks secrets put-secret olist aws_secret_access_key
from pyspark import pipelines as dp
from pyspark.sql import functions as F
import boto3
import io
import pandas as pd

aws_access_key_id = dbutils.secrets.get("olist", "aws_access_key_id")
aws_secret_access_key = dbutils.secrets.get("olist", "aws_secret_access_key")
endpoint = "https://bhcddcorgiqoqtkmsczs.storage.supabase.co/storage/v1/s3"
region = "sa-east-1"

# 2. Create the S3 client
client = boto3.client('s3',
                       aws_access_key_id=aws_access_key_id,
                       aws_secret_access_key=aws_secret_access_key,
                       region_name=region,
                       endpoint_url=endpoint)

# 3. Ingestion: read every CSV from the bucket and publish it as a raw bronze table.
#    Bronze is the raw layer: no transformations, types stay as-is for silver to fix.
#    In a pipeline, each table is a declared dataset (materialized view) and the
#    pipeline manages the writes — no .write / saveAsTable.

# Files are split across two buckets (discovered earlier):
#   olist2 -> customers, order_items, payments
#   olist  -> orders, reviews, products, sellers, category_translation
# Source file (Kaggle Olist dataset hosted in Supabase) -> bronze table
FILES = {
    ("olist2", "olist_customers_dataset.csv"): "customers",
    ("olist", "olist_orders_dataset.csv"): "orders",
    ("olist2", "olist_order_items_dataset.csv"): "order_items",
    ("olist2", "olist_order_payments_dataset.csv"): "payments",
    ("olist", "olist_order_reviews_dataset.csv"): "reviews",
    ("olist", "olist_products_dataset.csv"): "products",
    ("olist", "olist_sellers_dataset.csv"): "sellers",
    ("olist", "product_category_name_translation.csv"): "category_translation",
}

def _load_bronze_csv(bucket, key):
    response = client.get_object(Bucket=bucket, Key=key)
    pdf = pd.read_csv(io.BytesIO(response["Body"].read()))

    # The serverless pipeline runtime keeps pandas NaN as-is (string 'nan' / double NaN)
    # instead of NULL, unlike the notebook cluster. Normalize missing values to NULL:
    #  - object columns: NaN -> None (unambiguous in pandas)
    #  - double columns: NaN -> NULL via isnan (real numbers are never NaN)
    for col in pdf.columns:
        if pdf[col].dtype == object:
            pdf[col] = pdf[col].where(pdf[col].notnull(), None)

    df = spark.createDataFrame(pdf)
    for c, t in df.dtypes:
        if t == 'double':
            df = df.withColumn(c, F.when(F.isnan(c), F.lit(None)).otherwise(F.col(c)).cast(t))
    return df

@dp.materialized_view(name="customers")
def bronze_customers():
    return _load_bronze_csv("olist2", "olist_customers_dataset.csv")

@dp.materialized_view(name="orders")
def bronze_orders():
    return _load_bronze_csv("olist", "olist_orders_dataset.csv")

@dp.materialized_view(name="order_items")
def bronze_order_items():
    return _load_bronze_csv("olist2", "olist_order_items_dataset.csv")

@dp.materialized_view(name="payments")
def bronze_payments():
    return _load_bronze_csv("olist2", "olist_order_payments_dataset.csv")

@dp.materialized_view(name="reviews")
def bronze_reviews():
    return _load_bronze_csv("olist", "olist_order_reviews_dataset.csv")

@dp.materialized_view(name="products")
def bronze_products():
    return _load_bronze_csv("olist", "olist_products_dataset.csv")

@dp.materialized_view(name="sellers")
def bronze_sellers():
    return _load_bronze_csv("olist", "olist_sellers_dataset.csv")

@dp.materialized_view(name="category_translation")
def bronze_category_translation():
    return _load_bronze_csv("olist", "product_category_name_translation.csv")