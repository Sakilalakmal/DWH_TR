/* ============================================================
   Procedure: bronze.load_bronze
   Purpose  : Load raw CSV files into Bronze layer tables
   Usage    : EXEC bronze.load_bronze;
   ============================================================ */

CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @StartTime DATETIME2 = SYSDATETIME();

    BEGIN TRY

        PRINT '=======================================================================================';
        PRINT 'Starting Bronze Layer Load Process...';
        PRINT '=======================================================================================';

        /* ============================================================
           CRM SOURCE TABLES
           ============================================================ */

        PRINT '---------------------------------------------------------------------------------------';
        PRINT 'Loading CRM Tables...';
        PRINT '---------------------------------------------------------------------------------------';

        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\USER\Music\DW\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );


        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\USER\Music\DW\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );


        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\Users\USER\Music\DW\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );


        /* ============================================================
           ERP SOURCE TABLES
           ============================================================ */

        PRINT '---------------------------------------------------------------------------------------';
        PRINT 'Loading ERP Tables...';
        PRINT '---------------------------------------------------------------------------------------';

        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\USER\Music\DW\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );


        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\USER\Music\DW\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );


        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\USER\Music\DW\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            TABLOCK
        );


        DECLARE @EndTime DATETIME2 = SYSDATETIME();

        PRINT '=======================================================================================';
        PRINT 'Bronze Layer Load Completed Successfully.';
        PRINT 'Execution Time (seconds): ' 
              + CAST(DATEDIFF(SECOND, @StartTime, @EndTime) AS VARCHAR(20));
        PRINT '=======================================================================================';

    END TRY

    BEGIN CATCH

        PRINT '=======================================================================================';
        PRINT 'ERROR OCCURRED DURING BRONZE LOAD';
        PRINT 'Error Number   : ' + CAST(ERROR_NUMBER() AS VARCHAR(10));
        PRINT 'Error Message  : ' + ERROR_MESSAGE();
        PRINT 'Error Line     : ' + CAST(ERROR_LINE() AS VARCHAR(10));
        PRINT '=======================================================================================';

        THROW;

    END CATCH
END;
GO

EXEC bronze.load_bronze;