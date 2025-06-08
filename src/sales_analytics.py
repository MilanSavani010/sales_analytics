import sqlite3
import csv
import logging
import pandas as pd

logging.basicConfig(filename='../logs/sales_analytics.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

DB_NAME = '../databases/sales.db'


def setup_database():
    # Process
    #1. connect database
    #2. assign cursor
    #3. execute cursor (create table)
    #4. insert databases( cursor )
    #5. commit
    #6. disconnect database

    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS sales (
            order_id INTEGER PRIMARY KEY,
            order_date TEXT,
            customer_id INTEGER,
            revenue REAL,
            product_category TEXT
        )
    ''')

    sample_data = [
        (1, '2025-06-01', 101, 250.0, 'Car Rental'),
        (2, '2025-06-02', 102, 300.0, 'Car Subscription'),
        (3, '2025-06-03', 101, 200.0, 'Car Rental'),
        (4, '2025-06-04', 103, 400.0, 'Car Subscription')
    ]
    cursor.executemany('INSERT OR REPLACE INTO sales VALUES (?, ?, ?, ?, ?)', sample_data)
    conn.commit()
    conn.close()

def run_analytics():
    # Process
    # 1. connect database
    # 2. define queries
    # 3. read queries and translate to dataframe
    # 4. disconnect database
    conn = sqlite3.connect(DB_NAME)
    queries = {
        'total_revenue': 'SELECT SUM(revenue) AS total_revenue FROM sales',
        'orders_by_category': '''
            SELECT product_category, COUNT(*) AS order_count, SUM(revenue) AS category_revenue
            FROM sales GROUP BY product_category
        ''',
        'repeat_customers': '''
            SELECT customer_id, COUNT(*) AS order_count
            FROM sales GROUP BY customer_id HAVING order_count > 1
        '''
    }
    results = {}
    try:
        for name, query in queries.items():
            df = pd.read_sql_query(query, conn)
            results[name] = df
            logging.info(f"Executed query: {name}")
        conn.close()
        return results
    except Exception as e:
        logging.error(f"Query error: {e}")
        conn.close()
        return None

def main():
    setup_database()
    run_analytics()

if __name__ == '__main__':
    main()