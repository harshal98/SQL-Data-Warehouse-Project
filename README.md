# Data Warehouse Project
This project demonstrates the implementation of an ETL (Extract, Transform, Load) process and the movement of data through different layers of a data warehouse. It showcases how raw data is extracted from source systems, transformed into a structured and meaningful format, and finally loaded into a data warehouse for reporting and analytics.

## Data Architecture

The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
![Data Architecture](docs/DataWarehouse.png)
1. **Bronze Layer**: Stores raw data as-is from the source systems. Data is ingested from CSV Files into SQL Server Database.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.
