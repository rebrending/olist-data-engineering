from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'asmdan',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

with DAG(
    dag_id='olist_dwh_pipeline',
    default_args=default_args,
    description='ETL пайплайн Olist: Staging -> Core -> Data Marts',
    schedule_interval=None,  # Запуск по кнопке для разработки
    catchup=False,
) as dag:

    # -------------------------------------------------------------------------
    # TASK 1: Загрузка сырых CSV в схему raw (Staging)
    # -------------------------------------------------------------------------
    load_raw_data = BashOperator(
        task_id='load_raw_data',
        bash_command='cd ~/data_engineering && source venv/bin/activate && python scripts/load_raw.py',
    )

    # -------------------------------------------------------------------------
    # TASK 2: Создание витрины ежедневной выручки и накопительного итога
    # -------------------------------------------------------------------------
    # Используем CTE и оконную функцию SUM(...) OVER(...)
    create_daily_revenue_mart = PostgresOperator(
        task_id='create_daily_revenue_mart',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE SCHEMA IF NOT EXISTS marts;
            DROP TABLE IF EXISTS marts.daily_revenue;

            CREATE TABLE marts.daily_revenue AS
            WITH daily_stats AS (
                SELECT 
                    DATE(TO_TIMESTAMP(o."order_purchase_timestamp", 'YYYY-MM-DD HH24:MI:SS')) AS order_date,
                    COUNT(DISTINCT o."order_id") AS orders_count,
                    ROUND(SUM(CAST(i."price" AS NUMERIC)), 2) AS daily_revenue
                FROM raw.orders o
                JOIN raw.order_items i ON o."order_id" = i."order_id"
                WHERE o."order_status" = 'delivered'
                  AND o."order_purchase_timestamp" IS NOT NULL
                GROUP BY 1
            )
            SELECT 
                order_date,
                orders_count,
                daily_revenue,
                -- Оконная функция: считаем нарастающий итог (Cumulative Revenue) с начала истории
                ROUND(SUM(daily_revenue) OVER (ORDER BY order_date ASC), 2) AS cumulative_revenue,
                -- Скользящее среднее выручки за 7 дней
                ROUND(AVG(daily_revenue) OVER (
                    ORDER BY order_date ASC 
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                ), 2) AS moving_avg_7d_revenue
            FROM daily_stats
            ORDER BY order_date ASC;
        """
    )

    # -------------------------------------------------------------------------
    # TASK 3: Витрина активности клиентов (Когорты и время между заказами)
    # -------------------------------------------------------------------------
    # Используем CTE, LAG(...) для поиска предыдущего заказа и MIN(...) OVER(...)
    create_customer_retention_mart = PostgresOperator(
        task_id='create_customer_retention_mart',
        postgres_conn_id='postgres_default',
        sql="""
            DROP TABLE IF EXISTS marts.customer_retention;

            CREATE TABLE marts.customer_retention AS
            WITH customer_orders AS (
                SELECT 
                    c."customer_unique_id",
                    o."order_id",
                    TO_TIMESTAMP(o."order_purchase_timestamp", 'YYYY-MM-DD HH24:MI:SS') AS purchase_time
                FROM raw.orders o
                JOIN raw.customers c ON o."customer_id" = c."customer_id"
                WHERE o."order_status" = 'delivered'
                  AND o."order_purchase_timestamp" IS NOT NULL
            ),
            ordered_sequence AS (
                SELECT 
                    customer_unique_id,
                    order_id,
                    purchase_time,
                    -- Когорта клиента (месяц первой покупки)
                    DATE_TRUNC('month', MIN(purchase_time) OVER (PARTITION BY customer_unique_id)) AS cohort_month,
                    -- Порядковый номер заказа для конкретного клиента
                    ROW_NUMBER() OVER (PARTITION BY customer_unique_id ORDER BY purchase_time ASC) AS order_seq_num,
                    -- Дата предыдущего заказа (через LAG)
                    LAG(purchase_time, 1) OVER (PARTITION BY customer_unique_id ORDER BY purchase_time ASC) AS prev_purchase_time
                FROM customer_orders
            )
            SELECT 
                customer_unique_id,
                order_id,
                DATE(purchase_time) AS purchase_date,
                DATE(cohort_month) AS cohort_month,
                order_seq_num,
                DATE(prev_purchase_time) AS prev_purchase_date,
                -- Количество дней с момента предыдущего заказа
                ROUND(EXTRACT(EPOCH FROM (purchase_time - prev_purchase_time)) / 86400, 1) AS days_since_prev_order
            FROM ordered_sequence
            ORDER BY cohort_month ASC, customer_unique_id, order_seq_num ASC;
        """
    )

    # -------------------------------------------------------------------------
    # Задаём зависимости (граф вычислений)
    # Сначала льём raw-данные, затем параллельно или последовательно строим витрины
    # -------------------------------------------------------------------------
    load_raw_data >> [create_daily_revenue_mart, create_customer_retention_mart]