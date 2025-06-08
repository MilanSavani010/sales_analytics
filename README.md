# Sales Analytics Project

## Overview
A Python-based analytics tool for analyzing sales data, checking data quality, and performing customer segmentation using SQLite database.

## Features
- **Sales Analytics**
  - Revenue calculations and trends
  - Product category analysis
  - Repeat customer identification
  - CSV report generation

- **Data Quality Checker**
  - Missing value detection
  - Negative revenue validation
  - Duplicate order ID checking
  - Markdown report generation

- **Customer Segmentation**
  - Revenue-based customer segmentation
  - Segment visualization
  - Detailed segmentation reporting

## Project Structure
```
├── databases/           # SQLite database files
├── logs/               # Application log files
├── plots/              # Generated visualization plots
├── reports/            # Generated analysis reports
└── src/                # Source code modules
```

## Installation
```sh
pip install -r requirements.txt
```

## Required Dependencies
- pandas==2.3.0
- matplotlib==3.10.3
- numpy==2.2.6
- tabulate==0.9.0
- Markdown==3.8

## Usage
```sh
# Run Sales Analytics
python src/sales_analytics.py

# Run Data Quality Check
python src/data_quality_checker.py

# Run Customer Segmentation
python src/customer_segmentation.py
```

## Testing
```sh
python -m unittest src/test_sales_analytics.py
python -m unittest src/test_data_quality_checker.py
python -m unittest src/test_customer_segmentation.py
```

## Output Files
- Reports: ./reports/
  - sales_report.csv
  - data_quality_report.md  
  - segmentation_report.md
- Plots: ./plots/
  - segmentation_plot.png
- Logs: ./logs/
  - sales_analytics.log
  - data_quality.log
  - segmentation.log

