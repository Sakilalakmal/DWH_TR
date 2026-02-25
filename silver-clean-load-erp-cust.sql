USE DB_WAREHOUSE

PRINT 'Cleaning and Loading ERP Customer Table (SILVER) --'
TRUNCATE TABLE silver.erp_cust_az12
PRINT 'Loading data into silver.erp_cust_az12...'
INSERT INTO silver.erp_cust_az12
    (
    cid ,
    bdate ,
    gen
    )
SELECT
    CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	 ELSE cid
END cid,
    CASE WHEN bdate > GETDATE() THEN NULL
     ELSE bdate
END bdate,
    CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
	 ELSE 'n/a'
END gen
FROM bronze.erp_cust_az12

-- data quality checking --
SELECT DISTINCT gen
FROM silver.erp_cust_az12

-- check data after cleaning --
SELECT *
FROM silver.erp_cust_az12 