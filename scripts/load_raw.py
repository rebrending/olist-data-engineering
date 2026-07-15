import os
import logging
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

DB_CONFIG = {
    "host": os.environ.get("DB_HOST", "de_postgres"),
    "port": os.environ.get("DB_PORT", 5432),
    "user": os.environ.get("DB_USER", "admin"),
    "password": os.environ.get("DB_PASSWORD", "admin_password"),
    "dbname": os.environ.get("DB_NAME", "analytics_db")
}

DATA_DIR = os.environ.get("DATA_DIR", "/opt/airflow/data")

TABLE_FILES_MAPPING = {
    "raw.olist_customers": "olist_customers_dataset.csv",
    "raw.olist_orders": "olist_orders_dataset.csv",
    "raw.olist_order_payments": "olist_order_payments_dataset.csv"
}

def load_csv_to_postgres():
    conn = None
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        logging.INFO("Successfully connected to PostgreSQL DWH.")

        for table_name, file_name in TABLE_FILES_MAPPING.items():
            file_path = os.path.join(DATA_DIR, file_name)
            if not os.path.exists(file_path):
                logging.warning(f"File {file_path} not found. Skipping {table_name}...")
                continue

            logging.info(f"Loading {file_name} into {table_name}...")
            df = pd.read_csv(file_path)
            df = df.where(pd.notnull(df), None)

            cur.execute(f"TRUNCATE TABLE {table_name};")
            
            columns = list(df.columns)
            query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES %s"
            
            records = [tuple(x) for x in df.to_numpy()]
            execute_values(cur, query, records, page_size=10000)
            conn.commit()
            logging.info(f"Loaded {len(records)} records into {table_name}.")

    except Exception as e:
        logging.error(f"ETL Pipeline Error: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            conn.close()
            logging.info("Database connection closed.")

if __name__ == "__main__":
    load_csv_to_postgres()