-- find the total sales -- 
SELECT SUM(sales_amount) FROM gold.fact_sales

-- find how many items are sold --
SELECT SUM(quantity) FROM gold.fact_sales

-- find the average selling price --
SELECT AVG(price) FROM gold.fact_sales

-- find the total number of orders --
SELECT COUNT(order_number) AS total_order FROM gold.fact_sales
SELECT COUNT(DISTINCT(order_number)) AS total_order FROM gold.fact_sales

-- find the total number of products --
SELECT COUNT(product_key) FROM gold.dim_products

--find the total number of customers --
SELECT COUNT(customer_key) AS total_customers FROM gold.dim_customers

-- customers who placed orders --
SELECT COUNT(DISTINCT(customer_key)) AS total_customers FROM gold.fact_sales

SELECT 'Total Sales' as measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL 
SELECT 'Total Quantity' , SUM(quantity)  FROM gold.fact_sales
UNION ALL
SELECT 'Average Price' , AVG(price)  FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Orders', COUNT(DISTINCT(order_number))  FROM gold.fact_sales
UNION ALL
SELECT 'Total Nr. Products', COUNT(product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total Nr. Customers', COUNT(DISTINCT(customer_key))  FROM gold.dim_customers