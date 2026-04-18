# Architecture Overview

This project uses an end-to-end data platform architecture for a ride-hailing business.

Ride events will be generated from simulated transport activity and optional API sources. The data will be ingested in batch and later in streaming mode, then stored in a MinIO-based data lake.

Processing will be done using Python and Spark, while trusted analytics-ready data will be loaded into PostgreSQL. DBT will manage warehouse transformations, and Airflow will orchestrate the workflows.

Prometheus and Grafana will monitor the platform, while Metabase will support business dashboards. Curated data from the warehouse will also be used to create machine learning feature datasets and models.
