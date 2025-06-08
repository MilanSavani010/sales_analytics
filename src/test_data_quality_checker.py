import unittest
import sqlite3
import os
import pandas as pd
from data_quality_checker import load_data, check_data_quality, generate_quality_report

class TestDataQualityChecker(unittest.TestCase):
    def setUp(self):
        """Set up a test database with sample data."""
        self.db_name = '../databases/sales.db'
        self.data_qaulity_report = '../reports/data_quality_report.md'


        if os.path.exists(self.db_name):
            os.remove(self.db_name)
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE sales (
                order_id INTEGER,
                order_date TEXT,
                customer_id INTEGER,
                revenue REAL,
                product_category TEXT
            )
        ''')
        sample_data = [
            (1, '2025-06-01', 101, 250.0, 'Car Rental'),
            (2, '2025-06-01', 101, -50.0, 'Car Rental'),  # Negative revenue
            (1, '2025-06-02', 102, 300.0, None)  # Duplicate order_id, missing category
        ]
        cursor.executemany('INSERT INTO sales VALUES (?, ?, ?, ?, ?)', sample_data)
        conn.commit()
        conn.close()

    def test_load_data(self):
        df = load_data()
        self.assertIsNotNone(df)
        self.assertEqual(len(df), 3)  # 3 rows inserted
        self.assertIn('revenue', df.columns)

    def test_check_data_quality(self):
        df = load_data()
        report = check_data_quality(df)
        self.assertGreaterEqual(len(report['issues']), 2)  # At least negative revenue and missing values
        self.assertTrue(any('Negative revenue' in issue for issue in report['issues']))
        self.assertTrue(any('Duplicate order IDs' in issue for issue in report['issues']))
        self.assertTrue(any('Missing values in product_category' in issue for issue in report['issues']))

    def test_generate_quality_report(self):
        df = load_data()
        report = check_data_quality(df)
        generate_quality_report(report)
        self.assertTrue(os.path.exists(self.data_qaulity_report))
        with open(self.data_qaulity_report, 'r') as f:
            content = f.read()
            self.assertIn('Data Quality Report', content)

    def tearDown(self):
        if os.path.exists(self.db_name):
            os.remove(self.db_name)
        if os.path.exists(self.data_qaulity_report):
            os.remove(self.data_qaulity_report)

if __name__ == '__main__':
    unittest.main()