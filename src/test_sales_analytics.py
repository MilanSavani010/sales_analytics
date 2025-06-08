import sqlite3
import os
import pandas as pd
from sales_analytics import setup_database, run_analytics, generate_report
import unittest

class TestSalesAnalytics(unittest.TestCase):
    def setUp(self):
        self.db_name = '../databases/sales.db'
        self.sales_report = "../reports/sales_report.csv"

        if os.path.exists(self.db_name):
            os.remove(self.db_name)
        setup_database()

    def test_database_creation(self):
        self.assertTrue(os.path.exists(self.db_name))
        conn = sqlite3.connect(self.db_name)
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='sales'")
        self.assertIsNotNone(cursor.fetchone())
        conn.close()

    def test_run_analytics(self):
        results = run_analytics()
        self.assertIn('total_revenue', results)
        self.assertIn('orders_by_category', results)
        self.assertIn('repeat_customers', results)
        self.assertEqual(len(results['total_revenue']), 1)
        self.assertGreaterEqual(len(results['orders_by_category']), 1)

    def test_generate_report(self):
        results = run_analytics()
        generate_report(results)
        self.assertTrue(os.path.exists(self.sales_report))
        with open(self.sales_report, 'r') as f:
            content = f.read()
            self.assertIn('total_revenue', content)
            self.assertIn('orders_by_category', content)

    def tearDown(self):
        if os.path.exists(self.db_name):
            os.remove(self.db_name)
        if os.path.exists(self.sales_report):
            os.remove(self.sales_report)

if __name__ == '__main__':
    unittest.main()