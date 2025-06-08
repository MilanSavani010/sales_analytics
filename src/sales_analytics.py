import sqlite3
import csv
import logging
import pandas as pd

logging.basicConfig(filename='../logs/sales_analytics.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

DB_NAME = '../data/sales.db'

def setup_database():
    """Create a sample SQLite database with sales data."""
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
    # Insert sample data
    sample_data = [
        (1, '2025-06-01', 101, 250.0, 'Car Rental'),
        (2, '2025-06-02', 102, 300.0, 'Car Subscription'),
        (3, '2025-06-03', 101, 200.0, 'Car Rental'),
        (4, '2025-06-04', 103, 400.0, 'Car Subscription')
    ]
    cursor.executemany('INSERT OR REPLACE INTO sales VALUES (?, ?, ?, ?, ?)', sample_data)
    conn.commit()
    conn.close()

def main():
    setup_database()

if __name__ == '__main__':
    main()