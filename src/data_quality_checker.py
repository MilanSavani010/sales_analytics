import pandas as pd
import sqlite3
import logging
import markdown

logging.basicConfig(filename='../logs/data_quality.log', level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s')

DB_NAME = '../databases/sales.db'

def load_data():
    # process
    #1. connect database
    #2. read all data transform into dataframe
    #3. disconnect database
    try:
        conn = sqlite3.connect(DB_NAME)
        df = pd.read_sql_query("SELECT * FROM sales", conn)
        conn.close()
        return df
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        return None



def main():
    df = load_data()
    if df is None:
        print("Failed to load data.")
        return

if __name__ == '__main__':
    main()