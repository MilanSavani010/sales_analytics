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
        (1, '2025-06-28', 104, 0.0, ''),
        (2, '2025-06-12', 103, 247.85, 'Car Subscription'),
        (3, '2025-06-23', None, 0.0, ''),
        (4, '2025-06-25', None, 0.0, 'Car Subscription'),
        (5, '2025-06-10', 103, -80.75, None),
        (6, None, 104, 194.01, None),
        (7, '2025-06-12', 102, 0.0, 'Car Rental'),
        (8, '2025-06-01', 104, 0.0, 'Car Subscription'),
        (9, '2025-06-29', 106, 0.0, None),
        (5, '2025-06-10', 103, -80.75, None),
        (11, '2025-06-04', 101, 150.0, 'Car Rental'),
        (12, '2025-06-07', None, -120.0, ''),
        (13, '2025-06-15', 105, 205.6, 'Car Subscription'),
        (14, '2025-06-20', 107, 0.0, None),
        (15, '2025-06-22', 101, -99.99, 'Car Rental'),
        (16, None, 102, 420.0, ''),
        (17, '2025-06-02', 103, 0.0, ''),
        (18, '2025-06-18', None, 110.0, 'Car Rental'),
        (19, '2025-06-13', 104, 250.5, 'Car Subscription'),
        (11, '2025-06-04', 101, 150.0, 'Car Rental'),
        (21, '2025-06-26', 106, 350.0, 'Car Rental'),
        (22, '2025-06-05', None, 0.0, None),
        (23, '2025-06-30', 101, -75.5, ''),
        (24, '2025-06-08', 102, 490.2, 'Car Subscription'),
        (25, None, 104, 0.0, ''),
        (26, '2025-06-14', None, -50.0, ''),
        (27, '2025-06-06', 103, 330.0, 'Car Rental'),
        (28, '2025-06-11', 105, 0.0, 'Car Subscription'),
        (29, '2025-06-03', 106, 120.5, ''),
        (30, '2025-06-09', 107, 0.0, None),
        (24, '2025-06-08', 102, 490.2, 'Car Subscription'),
        (31, '2025-06-04', 101, 150.0, 'Car Rental'),
        (32, '2025-06-12', 104, 300.0, None),
        (33, '2025-06-21', 105, -199.0, 'Car Subscription'),
        (34, '2025-06-17', 103, 480.0, 'Car Rental'),
        (35, None, None, 0.0, ''),
        (36, '2025-06-24', 101, -45.6, ''),
        (37, '2025-06-10', 104, 99.99, 'Car Rental'),
        (38, '2025-06-19', 106, 0.0, None),
        (39, '2025-06-01', 107, -130.0, ''),
        (40, '2025-06-07', None, 375.0, 'Car Subscription'),
        (33, '2025-06-21', 105, -199.0, 'Car Subscription'),
        (41, '2025-06-06', 103, 60.0, 'Car Rental'),
        (42, '2025-06-15', 101, 0.0, None),
        (43, '2025-06-20', 102, 250.0, ''),
        (44, '2025-06-11', None, 130.5, 'Car Subscription'),
        (45, None, 105, -180.75, 'Car Rental'),
        (46, '2025-06-29', 103, 410.1, ''),
        (47, '2025-06-13', 104, 0.0, 'Car Rental'),
        (48, '2025-06-17', 107, -65.0, None),
        (49, '2025-06-28', 106, 370.2, 'Car Subscription'),
        (50, '2025-06-18', 102, 0.0, ''),
        (41, '2025-06-06', 103, 60.0, 'Car Rental'),
        (51, '2025-06-22', 101, 299.99, 'Car Subscription'),
        (52, None, None, -100.0, ''),
        (53, '2025-06-14', 104, 200.0, None),
        (54, '2025-06-12', 106, 0.0, 'Car Rental'),
        (55, '2025-06-01', 102, 145.0, 'Car Subscription'),
        (56, '2025-06-19', 107, -20.0, ''),
        (57, '2025-06-27', 105, 0.0, None),
        (58, '2025-06-10', None, 215.5, ''),
        (59, '2025-06-03', 101, -110.0, 'Car Rental'),
        (60, None, 103, 275.0, None),
        (53, '2025-06-14', 104, 200.0, None),
        (61, '2025-06-05', 102, 80.0, 'Car Subscription'),
        (62, '2025-06-24', 104, -55.5, ''),
        (63, '2025-06-06', 105, 390.0, 'Car Rental'),
        (64, '2025-06-08', 103, 0.0, ''),
        (65, '2025-06-16', None, 160.0, 'Car Subscription'),
        (66, '2025-06-11', 101, 0.0, ''),
        (67, '2025-06-30', 102, 145.5, 'Car Rental'),
        (68, None, 104, 285.0, 'Car Subscription'),
        (69, '2025-06-14', 107, -95.5, ''),
        (70, '2025-06-27', 106, 0.0, None),
        (61, '2025-06-05', 102, 80.0, 'Car Subscription'),
        (71, '2025-06-09', 105, 210.0, 'Car Rental'),
        (72, '2025-06-07', 103, 0.0, ''),
        (73, '2025-06-28', None, -160.5, ''),
        (74, '2025-06-13', 104, 130.0, 'Car Subscription'),
        (75, None, 107, 185.0, None),
        (76, '2025-06-12', 102, 0.0, ''),
        (77, '2025-06-03', 101, -49.5, 'Car Rental'),
        (78, '2025-06-22', None, 0.0, 'Car Subscription'),
        (79, '2025-06-25', 105, 470.0, ''),
        (80, '2025-06-26', 103, -88.0, None),
        (77, '2025-06-03', 101, -49.5, 'Car Rental'),
        (81, '2025-06-04', 106, 145.0, 'Car Rental'),
        (82, None, None, 310.0, ''),
        (83, '2025-06-10', 102, 0.0, 'Car Subscription'),
        (84, '2025-06-15', 104, 150.0, ''),
        (85, '2025-06-09', 103, -115.0, 'Car Rental'),
        (86, '2025-06-30', 105, 0.0, None),
        (87, '2025-06-08', None, 275.0, ''),
        (88, '2025-06-02', 106, 195.0, 'Car Subscription'),
        (89, None, 101, 0.0, ''),
        (90, '2025-06-06', 104, -140.0, None),
        (91, '2025-06-18', 105, 130.0, ''),
        (92, '2025-06-29', 107, 0.0, 'Car Rental'),
        (93, '2025-06-05', None, 285.0, 'Car Pool'),
        (94, '2025-06-07', 102, 110.5, ''),
        (95, '2025-06-01', 101, 0.0, None),
        (96, None, 103, -75.0, ''),
        (97, '2025-06-11', 105, 210.0, 'Car Rental'),
        (98, '2025-06-17', 106, 0.0, 'Car Subscription'),
        (99, '2025-06-16', None, 180.0, ''),
        (100, '2025-06-28', 104, -35.5, None)
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

def generate_report(results):
    if not results:
        logging.error("No results to generate report.")
        return

    report_data = []
    # Add total revenue
    total_revenue = results['total_revenue']['total_revenue'].iloc[0]
    report_data.append({
        'Metric': 'Total Revenue',
        'Product Category': '',
        'Order Count': '',
        'Revenue': f'{total_revenue:.2f}',
        'Customer ID': '',
    })

    # Add orders by category
    for _, row in results['orders_by_category'].iterrows():
        report_data.append({
            'Metric': 'Orders by Category',
            'Product Category': row['product_category'],
            'Order Count': row['order_count'],
            'Revenue': f"{row['category_revenue']:.2f}",
            'Customer ID': '',
        })

        # Add repeat customers
    for _, row in results['repeat_customers'].iterrows():
        customer_id = 'Unknown' if pd.isna(row['customer_id']) else int(row['customer_id'])
        report_data.append({
            'Metric': 'Repeat Customers',
            'Product Category': '',
            'Order Count': row['order_count'],
            'Revenue': '',
            'Customer ID': customer_id,
        })


    with open('../reports/sales_report.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=['Metric', 'Product Category', 'Order Count',
                                               'Revenue', 'Customer ID'])
        writer.writeheader()
        for row in report_data:
            writer.writerow(row)

logging.info("Generated sales_report.csv")
def main():
    setup_database()
    results = run_analytics()
    if results:
        generate_report(results)
        print("Analytics report generated at: ../reports/sales_report.csv")
    else:
        print("Failed to generate analytics.")

if __name__ == '__main__':
    main()