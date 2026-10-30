# Silver products: join category translations (with supplementary entries) and add completeness flags.
# Grain: 1 row per product (product_id).
from pyspark import pipelines as dp
from pyspark.sql import functions as F


@dp.materialized_view(name="olist.silver.products")
def silver_products():
    products = spark.read.table("products")

    # 1. Categories missing from the official translation table — supplementary translations
    missing_translations = spark.createDataFrame(
        [
            ('pc_gamer', 'gamer_pc'),
            ('portateis_cozinha_e_preparadores_de_alimentos', 'portable_kitchen_and_food_preparers'),
        ],
        ['product_category_name', 'product_category_name_english'],
    )

    # 2. Full translation table = bronze translations + supplementary entries
    category_translation_full = (
        spark.read.table('category_translation')
        .unionByName(missing_translations)
    )

    silver_products = (
        products
        # 3. Left join products with the full category translation table
        .join(category_translation_full, on='product_category_name', how='left')
        .withColumn('product_name_length', F.col('product_name_lenght'))
        .withColumn('product_description_length', F.col('product_description_lenght'))
        # 4. Flag: physical dimensions missing
        .withColumn('is_physics_missing',
                   F.when((F.col('product_weight_g').isNull()) | (F.col('product_length_cm').isNull()) | (F.col('product_height_cm').isNull()) | (F.col('product_width_cm').isNull()), True).otherwise(False))
        # 5. Flag: incomplete product record (missing category, name, description, photos or dimensions)
        .withColumn('incomplete_product',
                   F.when((F.col('product_category_name').isNull()) | (F.col('product_name_lenght').isNull()) | (F.col('product_description_lenght').isNull()) | (F.col('product_photos_qty').isNull()) | (F.col('is_physics_missing') == True), True).otherwise(False))
        # 6. Flag: product without category
        .withColumn('is_category_null',
                    F.when(F.col("product_category_name").isNull(), True).otherwise(False))
    )
    return silver_products.select('product_id', 'product_category_name', 'product_category_name_english', 'product_name_length', 'product_description_length', 'product_photos_qty', 'product_weight_g', 'product_length_cm', 'product_height_cm', 'product_width_cm', 'is_physics_missing', 'incomplete_product', 'is_category_null')
