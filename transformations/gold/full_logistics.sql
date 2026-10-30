-- 1. Logistics fact: 1 row per delivered order
--    Answers: average delivery time by region, lateness rate, impact on review score
-- 2. Duration metrics are hour-aware (fractional days from unix timestamps)
CREATE OR REFRESH MATERIALIZED VIEW olist.gold.full_logistics AS
SELECT
    o.order_id,
    DATE(o.order_purchase_timestamp) AS purchase_date,
    o.order_delivered_carrier_date,
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    ROUND((unix_timestamp(o.order_delivered_carrier_date) - unix_timestamp(o.order_purchase_timestamp)) / 86400.0, 2) AS purchase_to_carrier_days,
    ROUND((unix_timestamp(o.order_delivered_customer_date) - unix_timestamp(o.order_delivered_carrier_date)) / 86400.0, 2) AS carrier_to_customer_days,
    ROUND((unix_timestamp(o.order_delivered_customer_date) - unix_timestamp(o.order_purchase_timestamp)) / 86400.0, 2) AS total_delivery_days,
    ROUND((unix_timestamp(o.order_delivered_customer_date) - unix_timestamp(o.order_estimated_delivery_date)) / 86400.0, 2) AS est_vs_actual_diff_days,
    o.is_late_delivery,
    CASE 
        WHEN o.is_late_delivery 
        THEN ROUND((unix_timestamp(o.order_delivered_customer_date) - unix_timestamp(o.order_estimated_delivery_date)) / 86400.0, 2)
        ELSE 0
    END AS lateness_days,
    c.customer_state,
    c.customer_region,
    si.seller_state,
    r.review_score
FROM olist.silver.orders o
JOIN olist.silver.customers c ON o.customer_id = c.customer_id
-- 3. State of the order's main seller: highest item value, deterministic tie-break by seller_id
LEFT JOIN (
    SELECT order_id, seller_state
    FROM (
        SELECT
            oi.order_id,
            s.seller_state,
            ROW_NUMBER() OVER (
                PARTITION BY oi.order_id
                ORDER BY SUM(oi.price) DESC, oi.seller_id ASC
            ) AS rn
        FROM olist.silver.order_items oi
        JOIN olist.silver.sellers s ON oi.seller_id = s.seller_id
        GROUP BY oi.order_id, s.seller_state, oi.seller_id
    ) ranked
    WHERE ranked.rn = 1
) si ON o.order_id = si.order_id

-- 4. Review score so the lateness-vs-evaluation question is answerable in this fact
LEFT JOIN (
    SELECT order_id, review_score
    FROM olist.gold.full_order
    WHERE review_score IS NOT NULL
) r ON o.order_id = r.order_id
-- 5. Delivered orders only: logistics metrics require completed deliveries
WHERE o.order_status = 'delivered'