USE DB_WAREHOUSE

--===========================================--
--Dimensions Exploration--
--===========================================--

-- explore all countries -- 
SELECT DISTINCT 
	country
FROM gold.dim_customers

-- explore all categories and all dimensions for categories -- 
SELECT DISTINCT 
	category,
	subcategory,
	product_name
FROM gold.dim_products
ORDER BY 1,2,3

-- identify the timestamps --
SELECT 
	MIN(order_date) AS first_order_date ,
	MAX(order_date) AS last_order_date,
	DATEDIFF(YEAR,MIN(order_date),MAX(order_date)) AS order_range
FROM gold.fact_sales

-- youngest and oldest customer -- 
SELECT 
MIN(birthdate) AS oldest_birthdate,
DATEDIFF(YEAR,MIN(birthdate),GETDATE()) AS oldest_cust_age,
MAX(birthdate) AS yougest_birthdate,
DATEDIFF(YEAR,MAX(birthdate),GETDATE()) AS youngest_cust_age
FROM gold.dim_customers