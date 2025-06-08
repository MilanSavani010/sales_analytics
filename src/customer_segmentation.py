import sqlite3
import pandas as pd
import logging
import matplotlib.pyplot as plt

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
def visualize_segments(df):
    # bar plot of customer segments
    segment_counts = df['segment'].value_counts()
    plt.figure(figsize=(8, 6))
    segment_counts.plot(kind='bar', color='skyblue')
    plt.title('Customer Segmentation by Revenue')
    plt.xlabel('Segment')
    plt.ylabel('Number of Customers')
    plt.savefig('../plots/segmentation_plot.png')
    plt.close()
    logging.info("Generated segmentation_plot.png")

def generate_segmentation_report(df):
    # report in md file for documentation
    content = "# Customer Segmentation Report\n\n"
    content += "## Summary\n"
    content += df['segment'].value_counts().to_markdown() + "\n\n"
    content += "![Segmentation Plot](../plots/segmentation_plot.png)"

    with open('../reports/segmentation_report.md', 'w') as f:
        f.write(content)
    logging.info("Generated segmentation_report.md")

def main():
    df = run_segmentation()
    if df is None:
        print("Failed to perform segmentation.")
        return
    visualize_segments(df)
    generate_segmentation_report(df)
    print("Segmentation report generated: segmentation_report.md")


if __name__ == "__main__":
    main()