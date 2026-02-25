USE DB_WAREHOUSE

-- Which 5 product perfoming high revenue ?
SELECT TOP 5
	p.product_name,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC

-- RANK ---

SELECT * FROM (
	SELECT 
	p.product_name,
	SUM(f.sales_amount) AS total_revenue,
	RANK() OVER(ORDER BY SUM(f.sales_amount) DESC) AS rank
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
) t
WHERE rank <= 5
-- What are the 5 worst  performing product in terms of sales 
SELECT TOP 5
	p.product_name,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
)

ORDER BY total_revenue 

-- find the top 5 customer with highest revenue and lowet 3 with lower order count

WITH CTE_customer_metrics AS (
	
	SELECT 
	cus.customer_key,
	cus.first_name,
	cus.last_name,
	SUM(fs.sales_amount) AS total_amount,
    COUNT(DISTINCT(fs.order_number)) AS total_orders
	FROM gold.fact_sales AS fs
	LEFT JOIN gold.dim_customers AS cus
	ON fs.customer_key = cus.customer_key
	GROUP BY cus.customer_key,
	cus.first_name,
	cus.last_name
),
ranked AS (
	SELECT * ,
	ROW_NUMBER() OVER(ORDER BY total_amount DESC) AS amount_rank,
	ROW_NUMBER() OVER(ORDER BY total_orders ASC) AS total_orders_low_3
	FROM CTE_customer_metrics
)
SELECT * FROM ranked 
WHERE amount_rank <= 5 OR total_orders_low_3 <=3 
ORDER BY amount_rank , total_orders_low_3