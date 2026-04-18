# Database Design

This document defines the planned data structure for the ride-hailing platform.

The project will use multiple data layers: source events, staging, warehouse, and later machine learning feature tables. Raw ride events will first be stored in the data lake, then transformed into structured warehouse tables in PostgreSQL.

The warehouse will use a fact-and-dimension design. Planned dimension tables include drivers, riders, locations, dates, and payment methods. Planned fact tables include ride events, trips, payments, and driver activity.

These tables will support analytics, dashboards, monitoring, and later machine learning use cases such as demand forecasting and cancellation prediction.
