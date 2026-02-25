PRINT 'Cleaning and Loading ERP Catalog Table (SILVER) --'
TRUNCATE TABLE silver.erp_px_cat_g1v2
PRINT 'Loading data into silver.erp_px_cat_g1v2...'
INSERT INTO silver.erp_px_cat_g1v2
    (
    id ,
    cat ,
    subcat ,
    maintenance
    )
SELECT
    id,
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2

-- Looking for unwanted spaces --

SELECT
    cat,
    subcat,
    maintenance
FROM bronze.erp_px_cat_g1v2
WHERE LEN(cat) != LEN(TRIM(cat)) OR LEN(subcat) != LEN(TRIM(subcat)) OR LEN(maintenance) != LEN(TRIM(maintenance))

SELECT
    DISTINCT maintenance
FROM bronze.erp_px_cat_g1v2

-- check the data --

SELECT *
FROM silver.erp_px_cat_g1v2