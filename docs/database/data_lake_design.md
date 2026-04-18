# Data Lake Design

This document describes how the ride-hailing platform will organize file-based storage in the data lake.

The data lake will contain raw, clean, curated, rejected, and archive zones. Raw data will store original ride events and API responses. Clean data will store validated and standardized records. Curated data will store analytics-ready datasets for warehouse loading, dashboards, and machine learning.

Rejected data will store invalid records that fail validation checks, while the archive zone will keep older files and backup data. The main file formats will be JSON for incoming events, CSV for sample data, and Parquet for cleaned and curated datasets.
