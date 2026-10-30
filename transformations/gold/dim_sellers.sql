-- 1. Seller dimension: 1 row per seller_id
--    Answers: top states by sellers, top revenue, lateness rate
-- 2. Rules: commercial metrics = valid orders (excludes canceled/unavailable);
--    delivery metrics = delivered orders only; reviews = all orders
CREATE OR REFRESH MATERIALIZED VIEW olist.gold.dim_sellers AS
SELECT
    s.seller_id,
    MAX(seller_city) AS seller_city,
    MAX(seller_state) AS seller_state,
    MAX(seller_zip_code_prefix) AS seller_zip_code_prefix,
    MAX(has_sales) AS has_sales,
    COALESCE(ROUND(MAX(oi.total_revenue), 2), 0) AS total_revenue,
    COALESCE(ROUND(MAX(oi.total_revenue) / MAX(oi.order_count), 2), 0) AS avg_revenue_per_order,
    COALESCE(MAX(oi.order_count), 0) AS order_count,
    COALESCE(MAX(oi.distinct_products), 0) AS distinct_products,
    COALESCE(MAX(oi.total_items_sold), 0) AS total_items_sold,
    ROUND(MAX(oi.avg_freight_value), 2) AS avg_freight_value,
    MAX(oi.no_freight_rate) AS no_freight_rate,
    COALESCE(MAX(oi.shipping_anomaly_count), 0) AS shipping_anomaly_count,
    MAX(fo.is_late_delivery) AS has_late_delivery,
    MAX(fo.est_vs_actual_diff_days) AS worst_delay_days,
    ROUND(try_divide(SUM(CASE WHEN fo.is_late_delivery THEN 1 ELSE 0 END), COUNT(fo.order_id)), 4) AS late_delivery_rate,
    ROUND(AVG(rv.review_score), 2) AS avg_review_score

FROM olist.silver.sellers s

-- 3. Commercial metrics per seller from valid orders (excludes canceled/unavailable)
LEFT JOIN (
    SELECT
        it.seller_id,
        COUNT(DISTINCT it.order_id) AS order_count,
        COUNT(DISTINCT it.product_id) AS distinct_products,
        COUNT(it.product_id) AS total_items_sold,
        AVG(it.freight_value) AS avg_freight_value,
        SUM(it.price) AS total_revenue,
        ROUND(SUM(CASE WHEN it.no_freight THEN 1 ELSE 0 END) / COUNT(*), 4) AS no_freight_rate,
        SUM(CASE WHEN it.shipping_limit_anomaly THEN 1 ELSE 0 END) AS shipping_anomaly_count
    FROM olist.silver.order_items it
    JOIN olist.silver.orders o ON it.order_id = o.order_id
    WHERE o.order_status NOT IN ('canceled', 'unavailable')
    GROUP BY it.seller_id
) oi ON s.seller_id = oi.seller_id

-- 4. Distinct (order, seller) pairs to drive delivery and review metrics
LEFT JOIN (
    SELECT DISTINCT
        order_id,
        seller_id
    FROM olist.silver.order_items
) oi2 ON oi2.seller_id = s.seller_id

-- 5. Delivery metrics from delivered orders only
LEFT JOIN (
    SELECT
        order_id,
        est_vs_actual_diff_days,
        is_late_delivery
    FROM olist.gold.full_order
    WHERE order_status = 'delivered'
) fo ON oi2.order_id = fo.order_id

-- 6. Review scores from all orders
LEFT JOIN (
    SELECT
        order_id,
        review_score
    FROM olist.gold.full_order
    WHERE review_score IS NOT NULL
) rv ON oi2.order_id = rv.order_id

GROUP BY s.seller_id