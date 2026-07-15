-- Формирование аналитических витрин данных

-- 1. Витрина ежедневной выручки
DROP TABLE IF EXISTS marts.daily_revenue;
CREATE TABLE marts.daily_revenue AS
SELECT
    o.order_purchase_timestamp::date AS order_date,
    SUM(p.payment_value) AS daily_revenue
FROM staging.orders o
JOIN staging.payments p ON o.order_id = p.order_id
WHERE o.order_status = 'delivered'
GROUP BY 1
ORDER BY 1;

-- 2. Витрина когортного анализа клиентов
DROP TABLE IF EXISTS marts.customer_retention;
CREATE TABLE marts.customer_retention AS
WITH user_orders AS (
    SELECT
        c.customer_unique_id,
        o.order_id,
        o.order_purchase_timestamp::date AS purchase_date,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id 
            ORDER BY o.order_purchase_timestamp
        ) AS order_seq_num,
        MIN(o.order_purchase_timestamp::date) OVER (
            PARTITION BY c.customer_unique_id
        ) AS cohort_date
    FROM staging.orders o
    JOIN staging.customers c ON o.customer_id = c.customer_id
    WHERE o.order_status = 'delivered'
),
calculated_intervals AS (
    SELECT
        *,
        DATE_TRUNC('month', cohort_date)::date AS cohort_month,
        LAG(purchase_date) OVER (
            PARTITION BY customer_unique_id 
            ORDER BY purchase_date
        ) AS prev_purchase_date
    FROM user_orders
)
SELECT
    cohort_month,
    customer_unique_id,
    (purchase_date - prev_purchase_date) AS days_since_prev_order,
    order_id,
    order_seq_num,
    prev_purchase_date,
    purchase_date
FROM calculated_intervals;