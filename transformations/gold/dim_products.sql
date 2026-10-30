-- 1. Product dimension: 1 row per product_id
--    Answers: top categories by revenue, average price, best/worst reviews
CREATE OR REFRESH MATERIALIZED VIEW olist.gold.dim_products AS
SELECT
    p.product_id,
    product_category_name,
    product_category_name_english,
    product_photos_qty,
    product_weight_g,
    product_height_cm * product_length_cm * product_width_cm AS volume_cm3,
    incomplete_product,
    is_category_null,
    COALESCE(oi.total_revenue, 0) AS total_revenue,
    ROUND(oi.avg_price, 2) AS avg_price,
    COALESCE(oi.ordered_count, 0) AS ordered_count,
    COALESCE(oi.unique_orders, 0) AS unique_orders,
    ROUND(r.avg_review_score, 2) AS avg_review_score
FROM olist.silver.products p

-- 2. Sales metrics from valid orders only (excludes canceled/unavailable)
LEFT JOIN (
    SELECT
    it.product_id,
    SUM(it.price) AS total_revenue,
    AVG(it.price) AS avg_price,
    COUNT(*) AS ordered_count,
    COUNT(DISTINCT it.order_id) AS unique_orders
    FROM olist.silver.order_items it
    JOIN olist.silver.orders o ON it.order_id = o.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY it.product_id
) oi on p.product_id = oi.product_id

-- 3. Average review score per product (one row per order, no item fan-out)
LEFT JOIN (
    SELECT
        oi_dist.product_id,
        ROUND(AVG(r.avg_score), 2) AS avg_review_score
    FROM (
        SELECT DISTINCT product_id, order_id
        FROM olist.silver.order_items
    ) oi_dist
    JOIN (
        SELECT order_id, AVG(review_score) AS avg_score
        FROM olist.silver.reviews
        GROUP BY order_id
    ) r ON oi_dist.order_id = r.order_id
    GROUP BY oi_dist.product_id
) r ON p.product_id = r.product_id