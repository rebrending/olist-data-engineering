import os
import pandas as pd
import psycopg2

DB_PARAMS = {
    "host": "localhost",
    "port": 5433,
    "database": "analytics_db",
    "user": "admin",
    "password": "admin_password"
}

DATA_DIR = os.path.expanduser("~/data_engineering/data")

datasets = {
    "olist_customers_dataset.csv": "customers",
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items"
}

def main():
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()
    
    try:
        # Создаем схему raw
        cur.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        conn.commit()
        print("Схема 'raw' успешно проверена/создана.")
        
        for file_name, table_name in datasets.items():
            file_path = os.path.join(DATA_DIR, file_name)
            
            if not os.path.exists(file_path):
                print(f"⚠️ Файл {file_name} не найден, пропускаем.")
                continue
                
            print(f"Загрузка {file_name} в таблицу raw.{table_name}...")
            
            df = pd.read_csv(file_path, nrows=5)
            
            # Указываем кавычки для безопасности
            columns = [f'"{col}" TEXT' for col in df.columns]
            create_table_sql = f'DROP TABLE IF EXISTS raw."{table_name}"; CREATE TABLE raw."{table_name}" ({", ".join(columns)});'
            
            cur.execute(create_table_sql)
            conn.commit()
            
            # ВАЖНО: говорим сессии Postgres смотреть внутрь схемы raw
            cur.execute("SET search_path TO raw, public;")
            
            with open(file_path, 'r', encoding='utf-8') as f:
                next(f) 
                # Теперь передаем только имя таблицы, без "raw."
                cur.copy_from(f, table_name, sep=',', null="")
                
            conn.commit()
            print(f"✅ Успешно загружено в raw.{table_name}.")
            
    finally:
        cur.close()
        conn.close()

if __name__ == "__main__":
    main()
