TRUNCATE TABLE silver.crm_prd_info;
INSERT INTO silver.crm_prd_info(
	prd_id ,
    cat_id ,
    prd_key ,
    prd_nm ,
    prd_cost ,
    prd_line ,
    prd_start_dt ,
    prd_end_dt 
)
SELECT 
prd_id,
REPLACE(SUBSTRING(prd_key,1,5),'-','_') AS cat_id,
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
	 ELSE 'n/a'
END AS prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info




/* WHERE REPLACE(SUBSTRING(prd_key,1,5),'-','_') NOT IN 
(SELECT 
DISTINCT id
FROM bronze.erp_px_cat_g1v2) */

-- quality checking -- 
-- Expectation : No Result

SELECT 
prd_id,
COUNT(*) AS dup_count
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1

SELECT 
id
FROM bronze.erp_px_cat_g1v2


SELECT sls_prd_key FROM bronze.crm_sales_details

--  checking any unwanted spaces --
SELECT * FROM silver.crm_prd_info WHERE prd_nm != TRIM(prd_nm)

-- checking any negative or any negative null values -- 
SELECT * FROM silver.crm_prd_info WHERE  prd_cost IS NULL

SELECT DISTINCT prd_line FROM silver.crm_prd_info

SELECT 
prd_id,
prd_key,
prd_nm,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prs_test_next_day
FROM bronze.crm_prd_info 
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509')

DROP TABLE silver.crm_prd_info;
select * from silver.crm_prd_info