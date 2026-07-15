-- Очистка и дедупликация данных во временном слое Staging

TRUNCATE TABLE staging.customers;
INSERT INTO staging.customers
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    customer_city,
    customer_state
FROM raw.olist_customers;

TRUNCATE TABLE staging.orders;
INSERT INTO staging.orders
SELECT DISTINCT
    order_id,
    customer_id,
    order_status,
    TO_TIMESTAMP(order_purchase_timestamp, 'YYYY-MM-DD HH24:MI:SS') AS order_purchase_timestamp
FROM raw.olist_orders
WHERE order_status IS NOT NULL;

TRUNCATE TABLE staging.payments;
INSERT INTO staging.payments
SELECT 
    order_id,
    payment_type,
    payment_value
FROM raw.olist_order_payments;