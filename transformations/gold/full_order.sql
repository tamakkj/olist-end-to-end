-- 1. Order fact: 1 row per order, enriched with items, payments, reviews and customer data
--    Answers: temporal evolution, average ticket, revenue by region, lateness vs review score
CREATE OR REFRESH MATERIALIZED VIEW olist.gold.full_order AS
SELECT
    o.order_id,
    c.customer_unique_id,
    o.order_status,
    DATE(o.order_purchase_timestamp) AS purchase_date,
    DATE_FORMAT(o.order_purchase_timestamp, 'yyyy-MM') AS purchase_month,
    DATE_FORMAT(o.order_purchase_timestamp, 'EEEE') AS purchase_weekday,
    HOUR(o.order_purchase_timestamp) AS purchase_hour,
    oi.item_count,
    oi.distinct_sellers,
    oi.distinct_products,
    oi.total_items_price,
    oi.total_freight_value,
    ROUND(oi.total_items_price + oi.total_freight_value, 2) AS total_order_value,
    p.total_payment_value,
    p.primary_payment_type,
    p.max_payment_installments,
    c.customer_city,
    c.customer_state,
    c.customer_region,
    c.is_repeat_customer,
    ROUND((unix_timestamp(o.order_delivered_customer_date) - unix_timestamp(o.order_purchase_timestamp)) / 86400.0, 2) AS delivery_time_days,
    ROUND((unix_timestamp(o.order_delivered_customer_date) - unix_timestamp(o.order_estimated_delivery_date)) / 86400.0, 2) AS est_vs_actual_diff_days,
    o.is_late_delivery,
    o.order_data_incomplete,
    o.delivery_order_invalid,
    o.missing_approval,
    r.review_score,
    r.has_review_comment,
    r.has_late_review
FROM olist.silver.orders o
LEFT JOIN olist.silver.customers c ON o.customer_id = c.customer_id
-- 2. Items aggregated per order: counts and financial totals
LEFT JOIN (
    SELECT
        order_id,
        COUNT(*) AS item_count,
        COUNT(DISTINCT seller_id) AS distinct_sellers,
        COUNT(DISTINCT product_id) AS distinct_products,
        ROUND(SUM(price), 2) AS total_items_price,
        ROUND(SUM(freight_value), 2) AS total_freight_value
    FROM olist.silver.order_items
    GROUP BY order_id
) oi ON o.order_id = oi.order_id

-- 3. Payments aggregated per order: total paid, main payment type, max installments
LEFT JOIN (
    SELECT
        totals.order_id,
        totals.total_payment_value,
        ranked.primary_payment_type,
        totals.max_payment_installments
    FROM (
        SELECT
            order_id,
            ROUND(SUM(payment_value), 2) AS total_payment_value,
            MAX(payment_installments) AS max_payment_installments
        FROM olist.silver.payments
        GROUP BY order_id
    ) totals
    JOIN (
        SELECT
            order_id,
            payment_type AS primary_payment_type,
            ROW_NUMBER() OVER (
                PARTITION BY order_id
                ORDER BY SUM(payment_value) DESC, payment_type ASC
            ) AS rn
        FROM olist.silver.payments
        GROUP BY order_id, payment_type
    ) ranked
        ON totals.order_id = ranked.order_id
       AND ranked.rn = 1
) p ON o.order_id = p.order_id
-- 4. Reviews aggregated per order: average score and quality flags
LEFT JOIN (
    SELECT
        order_id,
        ROUND(AVG(review_score), 2) AS review_score,
        BOOL_OR(review_comment_message IS NOT NULL) AS has_review_comment,
        BOOL_OR(is_late_review) AS has_late_review
    FROM olist.silver.reviews
    GROUP BY order_id
) r ON o.order_id = r.order_id
