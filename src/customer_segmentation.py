import sqlite3
import pandas as pd
import logging

logging.basicConfig(filename='../logs/segmentation.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

DB_NAME = '../databases/sales.db'


def run_segmentation():
    # process
    # query grp by customer id and count total revenue
    # connect
    # read db with query transform to data frame
    # segment according to total revenue
    query = '''
        SELECT customer_id, 
               COUNT(*) AS order_count, 
               SUM(revenue) AS total_revenue
        FROM sales
        GROUP BY customer_id
    '''
    try:
        conn = sqlite3.connect(DB_NAME)
        df = pd.read_sql_query(query, conn)
        conn.close()

        df['segment'] = pd.cut(df['total_revenue'],
                               bins=[0, 300, 600, float('inf')],
                               labels=['Low Value', 'Medium Value', 'High Value'])
        return df
    except Exception as e:
        logging.error(f"Segmentation error: {e}")
        return None

def main():
    df = run_segmentation()

if __name__ == "__main__":
    main()