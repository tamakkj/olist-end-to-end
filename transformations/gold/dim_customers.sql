-- 1. Customer dimension: 1 row per customer_unique_id
--    Answers: repeat vs one-time buyers, spending comparison, top states
-- 2. Rules: total_spent/avg_spent = valid orders only (excludes canceled/unavailable);
--    order_count, flags and reviews = all orders
CREATE OR REFRESH MATERIALIZED VIEW olist.gold.dim_customers AS
SELECT
    c.customer_unique_id,
    MAX(c.customer_state) AS customer_state,
    MAX(c.customer_city) AS customer_city,
    MAX(c.customer_region) AS customer_region,
    MAX(c.location_inconsistent) AS location_inconsistent,
    MAX(c.is_repeat_customer) AS is_repeat_customer,
    MAX(c.has_canceled_order) AS has_canceled_order,
    COUNT(DISTINCT c.order_id) AS order_count,
    COALESCE(ROUND(MAX(sp.total_spent), 2), 0) AS total_spent,
    ROUND(MAX(sp.avg_spent), 2) AS avg_spent,
    MAX(sp.avg_review_score) AS avg_review_score
FROM olist.silver.customers c

-- 3. Spending and review metrics per customer, computed from the order fact
LEFT JOIN (
    SELECT
        customer_unique_id,
        SUM(CASE WHEN order_status NOT IN ('canceled', 'unavailable') THEN total_payment_value END) AS total_spent,
        AVG(CASE WHEN order_status NOT IN ('canceled', 'unavailable') THEN total_payment_value END) AS avg_spent,
        ROUND(AVG(review_score), 2) AS avg_review_score
    FROM olist.gold.full_order
    GROUP BY customer_unique_id
) sp ON c.customer_unique_id = sp.customer_unique_id
GROUP BY c.customer_unique_id
