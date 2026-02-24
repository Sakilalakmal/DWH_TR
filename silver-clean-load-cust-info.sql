--checks nulls or duplicate on primary key -- 
-- expectation: No Result -- 

SELECT
cst_id,
COUNT(*)
FROM silver.crm_cust_info
group by cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL

-- Data Standardization & Consistency --

SELECT
	DISTINCT cst_gender
FROM silver.crm_cust_info

SELECT
	DISTINCT cst_material_status
FROM silver.crm_cust_info

-- checking for unwanted spaces --
-- expectation: No Result
SELECT 
	cst_firstname
FROM silver.crm_cust_info 
WHERE LEN(cst_firstname) != LEN(TRIM(cst_firstname))

SELECT 
	cst_gender
FROM silver.crm_cust_info 
WHERE LEN(cst_gender) != LEN(TRIM(cst_gender))


-- filtered result only where cst_key (PK) is unique -- 
-- Cleaned cust info table (SILVER) --

INSERT INTO silver.crm_cust_info(
cst_id,
cst_key,
cst_firstname,
cst_lastname,
cst_material_status,
cst_gender,
cst_create_date
)
SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) AS cst_lastname,
CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
	 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
	 ELSE 'n/a'
END cst_material_status,
CASE WHEN UPPER(TRIM(cst_gender)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(cst_gender)) = 'M' THEN 'Male'
	 ELSE 'n/a'
END cst_gender,
cst_create_date
FROM (
	SELECT 
		*,
	ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS rn
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
)  t WHERE rn = 1


-- select all details --

select * from silver.crm_cust_info


