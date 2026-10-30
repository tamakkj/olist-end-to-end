# Silver reviews: deduplicate by review_id, cast types, normalize text fields and flag late reviews.
# Grain: 1 row per review (review_id).
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="olist.silver.reviews")
def silver_reviews():
    reviews = spark.read.table("reviews")
    return (
        reviews
        # 1. Remove duplicate reviews by review_id
        .dropDuplicates(['review_id'])

        # 2. Cast types and normalize text fields (trim, empty string becomes NULL)
        .withColumn('review_creation_date', F.col('review_creation_date').cast('date'))
        .withColumn('review_answer_timestamp', F.to_timestamp('review_answer_timestamp'))
        .withColumn('review_score', F.col('review_score').cast('int'))
        .withColumn('review_comment_title', F.trim('review_comment_title'))
        .withColumn('review_comment_message', F.trim('review_comment_message'))
        .withColumn('review_comment_title', F.when(
                F.col('review_comment_title') == '', None
            ).otherwise(F.col('review_comment_title')))
        .withColumn('review_comment_message', F.when(
                F.col('review_comment_message') == '', None
            ).otherwise(F.col('review_comment_message')))

        # 3. Flag: late review (answered more than 30 days after creation)
        .withColumn('is_late_review',
                    F.when(
                        (F.unix_timestamp('review_answer_timestamp') - F.unix_timestamp('review_creation_date')) / 86400.0 > 30, True).otherwise(False))
    )
