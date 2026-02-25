# DB_WAREHOUSE — Data Warehouse (Medallion Architecture)

A SQL Server data warehouse following the **Bronze → Silver → Gold** medallion pattern.

## Data Flow
<img width="700" height="400" alt="image" src="https://github.com/user-attachments/assets/a1a7fe7e-40fe-40be-a8df-dfb8e12d1b50" />

## Folder Structure

```
bronze/   Raw data ingestion — tables & bulk-insert scripts
silver/   Cleaned & conformed data — transformation scripts
gold/     Business-ready views — dimensions & facts
docs/     Documentation & data dictionaries
```

## Execution Order

### 1. Bronze Layer

1. `bronze/ddl_bronze_tables.sql` — Create raw tables
2. `bronze/load_bronze_bulk_insert.sql` — Bulk-insert source data

### 2. Silver Layer

1. `silver/ddl_silver_tables.sql` — Create cleaned tables
2. Run the `silver/load_*.sql` scripts to clean & load each source table
3. `silver/load_silver_final_insert.sql` — Final silver data insert

### 3. Gold Layer

1. `gold/dim_customers.sql` — Customer dimension view
2. `gold/dim_products.sql` — Product dimension view
3. `gold/fact_sales.sql` — Sales fact view
