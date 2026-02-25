
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	SET NOCOUNT ON;

	DECLARE @step_start_time DATETIME, @step_end_time DATETIME;
	DECLARE @proc_start_time DATETIME = GETDATE();
	DECLARE @step_duration_ms INT;
	DECLARE @total_duration_ms INT;

	PRINT '========================================';
	PRINT 'Silver Layer Load - Started at ' + CONVERT(VARCHAR, @proc_start_time, 121);
	PRINT '========================================';

	-- ============================================================
	-- 1) crm_cust_info
	-- ============================================================
	BEGIN TRY
		SET @step_start_time = GETDATE();
		PRINT '>> Cleaning and Loading Customer Info Table (silver.crm_cust_info)...';

		TRUNCATE TABLE silver.crm_cust_info;

		INSERT INTO silver.crm_cust_info
			(
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
		)  t
		WHERE rn = 1;

		SET @step_end_time = GETDATE();
		SET @step_duration_ms = DATEDIFF(MILLISECOND, @step_start_time, @step_end_time);
		PRINT '   [OK] silver.crm_cust_info loaded in ' + CAST(@step_duration_ms AS VARCHAR) + ' ms';
	END TRY
	BEGIN CATCH
		PRINT '   [ERROR] Failed to load silver.crm_cust_info: ' + ERROR_MESSAGE();
		PRINT '   Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR) + ' | Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR) + ' | State: ' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH;

	-- ============================================================
	-- 2) crm_prd_info
	-- ============================================================
	BEGIN TRY
		SET @step_start_time = GETDATE();
		PRINT '>> Cleaning and Loading Product Info Table (silver.crm_prd_info)...';

		TRUNCATE TABLE silver.crm_prd_info;

		INSERT INTO silver.crm_prd_info
			(
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
		FROM bronze.crm_prd_info;

		SET @step_end_time = GETDATE();
		SET @step_duration_ms = DATEDIFF(MILLISECOND, @step_start_time, @step_end_time);
		PRINT '   [OK] silver.crm_prd_info loaded in ' + CAST(@step_duration_ms AS VARCHAR) + ' ms';
	END TRY
	BEGIN CATCH
		PRINT '   [ERROR] Failed to load silver.crm_prd_info: ' + ERROR_MESSAGE();
		PRINT '   Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR) + ' | Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR) + ' | State: ' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH;

	-- ============================================================
	-- 3) crm_sales_details
	-- ============================================================
	BEGIN TRY
		SET @step_start_time = GETDATE();
		PRINT '>> Cleaning and Loading Sales Details Table (silver.crm_sales_details)...';

		TRUNCATE TABLE silver.crm_sales_details;

		INSERT INTO silver.crm_sales_details
			(
			sls_ord_num ,
			sls_prd_key ,
			sls_cust_id ,
			sls_order_dt ,
			sls_ship_dt ,
			sls_due_dt ,
			sls_sales ,
			sls_quantity ,
			sls_price
			)
		SELECT
			sls_ord_num,
			sls_prd_key,
			sls_cust_id,
			CASE WHEN sls_order_dt = 0
				OR LEN(sls_order_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END sls_order_dt,
			CASE WHEN sls_ship_dt = 0
				OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END sls_ship_dt,
			CASE WHEN sls_due_dt = 0
				OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END sls_due_dt,
			CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			 THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
			sls_quantity,
			CASE WHEN sls_price  IS NULL OR sls_price <= 0
			 THEN sls_sales/NULLIF(sls_quantity,0)
			 ELSE sls_price
		END AS sls_price
		FROM bronze.crm_sales_details;

		SET @step_end_time = GETDATE();
		SET @step_duration_ms = DATEDIFF(MILLISECOND, @step_start_time, @step_end_time);
		PRINT '   [OK] silver.crm_sales_details loaded in ' + CAST(@step_duration_ms AS VARCHAR) + ' ms';
	END TRY
	BEGIN CATCH
		PRINT '   [ERROR] Failed to load silver.crm_sales_details: ' + ERROR_MESSAGE();
		PRINT '   Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR) + ' | Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR) + ' | State: ' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH;

	-- ============================================================
	-- 4) erp_cust_az12
	-- ============================================================
	BEGIN TRY
		SET @step_start_time = GETDATE();
		PRINT '>> Cleaning and Loading ERP Customer Table (silver.erp_cust_az12)...';

		TRUNCATE TABLE silver.erp_cust_az12;

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
		FROM bronze.erp_cust_az12;

		SET @step_end_time = GETDATE();
		SET @step_duration_ms = DATEDIFF(MILLISECOND, @step_start_time, @step_end_time);
		PRINT '   [OK] silver.erp_cust_az12 loaded in ' + CAST(@step_duration_ms AS VARCHAR) + ' ms';
	END TRY
	BEGIN CATCH
		PRINT '   [ERROR] Failed to load silver.erp_cust_az12: ' + ERROR_MESSAGE();
		PRINT '   Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR) + ' | Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR) + ' | State: ' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH;

	-- ============================================================
	-- 5) erp_loc_a101
	-- ============================================================
	BEGIN TRY
		SET @step_start_time = GETDATE();
		PRINT '>> Cleaning and Loading ERP Location Table (silver.erp_loc_a101)...';

		TRUNCATE TABLE silver.erp_loc_a101;

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
		FROM bronze.erp_loc_a101;

		SET @step_end_time = GETDATE();
		SET @step_duration_ms = DATEDIFF(MILLISECOND, @step_start_time, @step_end_time);
		PRINT '   [OK] silver.erp_loc_a101 loaded in ' + CAST(@step_duration_ms AS VARCHAR) + ' ms';
	END TRY
	BEGIN CATCH
		PRINT '   [ERROR] Failed to load silver.erp_loc_a101: ' + ERROR_MESSAGE();
		PRINT '   Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR) + ' | Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR) + ' | State: ' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH;

	-- ============================================================
	-- 6) erp_px_cat_g1v2
	-- ============================================================
	BEGIN TRY
		SET @step_start_time = GETDATE();
		PRINT '>> Cleaning and Loading ERP Catalog Table (silver.erp_px_cat_g1v2)...';

		TRUNCATE TABLE silver.erp_px_cat_g1v2;

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
		FROM bronze.erp_px_cat_g1v2;

		SET @step_end_time = GETDATE();
		SET @step_duration_ms = DATEDIFF(MILLISECOND, @step_start_time, @step_end_time);
		PRINT '   [OK] silver.erp_px_cat_g1v2 loaded in ' + CAST(@step_duration_ms AS VARCHAR) + ' ms';
	END TRY
	BEGIN CATCH
		PRINT '   [ERROR] Failed to load silver.erp_px_cat_g1v2: ' + ERROR_MESSAGE();
		PRINT '   Error Number: ' + CAST(ERROR_NUMBER() AS VARCHAR) + ' | Severity: ' + CAST(ERROR_SEVERITY() AS VARCHAR) + ' | State: ' + CAST(ERROR_STATE() AS VARCHAR);
	END CATCH;

	-- ============================================================
	-- Total Duration Summary
	-- ============================================================
	SET @total_duration_ms = DATEDIFF(MILLISECOND, @proc_start_time, GETDATE());
	PRINT '========================================';
	PRINT 'Silver Layer Load - Completed';
	PRINT 'Total Duration: ' + CAST(@total_duration_ms AS VARCHAR) + ' ms (' + CAST(@total_duration_ms / 1000 AS VARCHAR) + ' seconds)';
	PRINT '========================================';
END

EXEC silver.load_silver