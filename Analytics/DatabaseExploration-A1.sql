USE DB_WAREHOUSE

-- explore all objects in the database
SELECT * FROM INFORMATION_SCHEMA.TABLES

-- explore all columns in the database
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers'