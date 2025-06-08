import unittest
import sqlite3
import os
import pandas as pd
from customer_segmentation import run_segmentation, visualize_segments, generate_segmentation_report

class TestCustomerSegmentation(unittest.TestCase):
    def setUp(self):
        self.db_name = '../databases/sales.db'
        self.plot ='../plots/segmentation_plot.png'
        self.seg_report ='../reports/segmentation_report.md'

        if os.path.exists(self.db_name):
            os.remove(self.db_name)
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE sales (
                order_id INTEGER PRIMARY KEY,
                order_date TEXT,
                customer_id INTEGER,
                revenue REAL,
                product_category TEXT
            )
        ''')
        sample_data = [
            (1, '2025-06-01', 101, 250.0, 'Car Rental'),
            (2, '2025-06-02', 101, 200.0, 'Car Rental'),
            (3, '2025-06-03', 102, 500.0, 'Car Subscription')
        ]
        cursor.executemany('INSERT INTO sales VALUES (?, ?, ?, ?, ?)', sample_data)
        conn.commit()
        conn.close()

    def test_run_segmentation(self):
        df = run_segmentation()
        self.assertIsNotNone(df)
        self.assertIn('segment', df.columns)
        self.assertEqual(df[df['customer_id'] == 101]['segment'].iloc[0], 'Medium Value')
        self.assertEqual(df[df['customer_id'] == 102]['segment'].iloc[0], 'Medium Value')

    def test_visualize_segments(self):
        """Test if the segmentation plot is generated."""
        df = run_segmentation()
        visualize_segments(df)
        self.assertTrue(os.path.exists(self.plot))

    def test_generate_segmentation_report(self):
        df = run_segmentation()
        generate_segmentation_report(df)
        self.assertTrue(os.path.exists(self.seg_report))
        with open(self.seg_report, 'r') as f:
            content = f.read()
            self.assertIn('Customer Segmentation Report', content)
            self.assertIn('Medium Value', content)

    def tearDown(self):
        if os.path.exists(self.db_name):
            os.remove(self.db_name)
        if os.path.exists(self.plot):
            os.remove(self.plot)
        if os.path.exists(self.seg_report):
            os.remove(self.seg_report)

if __name__ == '__main__':
    unittest.main()