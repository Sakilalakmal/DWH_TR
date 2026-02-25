PRINT 'Cleaning and Loading ERP Location Table (SILVER) --'
TRUNCATE TABLE silver.erp_loc_a101
PRINT 'Loading data into silver.erp_loc_a101...'
INSERT INTO silver.erp_loc_a101
    (
    cid ,
    cntry
    )
SELECT
    REPLACE(cid,'-','')cid,
    CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END cntry
FROM bronze.erp_loc_a101

-- checking with customer table --
-- WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM silver.crm_cust_info)

-- data standardization & Consistency --
SELECT DISTINCT cntry
FROM bronze.erp_loc_a101

-- after
SELECT DISTINCT cntry
FROM silver.erp_loc_a101

-- check all data in the silver.erp_loc_a101 (SILVER) --
SELECT *
FROM silver.erp_loc_a101