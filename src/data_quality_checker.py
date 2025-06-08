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
        df = pd.read_sql_query("SELECT * FROM sales", conn,index_col=None)
        conn.close()
        return df
    except Exception as e:
        logging.error(f"Failed to load data: {e}")
        return None


def check_data_quality(df):
    # process
    # missing value
    # negative revenue
    # duplicates
    # append issues to report
    report = {"issues": []}

    missing = df.isnull().sum()
    for column, count in missing.items():
        if count > 0:
            report["issues"].append(f"Missing values in {column}: {count}")

    negative_revenue = df[df['revenue'] < 0]
    if not negative_revenue.empty:
        report["issues"].append(f"Negative revenue found: {len(negative_revenue)} rows")

    duplicates = df[df.duplicated(['order_id'])]
    if not duplicates.empty:
        report["issues"].append(f"Duplicate order IDs: {len(duplicates)} rows")

    return report

def generate_quality_report(report):
    # quality report in md for documentation
    content = "# Data Quality Report\n\n"
    if report["issues"]:
        content += "## Issues Found\n"
        for issue in report["issues"]:
            content += f"- {issue}\n"
    else:
        content += "No data quality issues found.\n"

    with open('../reports/data_quality_report.md', 'w') as f:
        f.write(content)
    logging.info("Generated data_quality_report.md")

def main():
    df = load_data()
    if df is None:
        print("Failed to load data.")
        return
    report = check_data_quality(df)
    generate_quality_report(report)

if __name__ == '__main__':
    main()