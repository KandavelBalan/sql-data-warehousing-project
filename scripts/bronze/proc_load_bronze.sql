/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze_layer.load_bronze_layer;
===============================================================================
*/

CREATE OR ALTER PROCEDURE BRONZE_LAYER.LOAD_BRONZE_LAYER AS
BEGIN
	DECLARE @START_TIME DATETIME, @END_TIME DATETIME, @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;
	BEGIN TRY
	
		SET @BATCH_START_TIME = GETDATE();

		print '================================================';
		print 'Loading the bronze layer';
		print '================================================';

		print '------------------------------------------------';
		print 'Loading the CRM Tables';
		print '------------------------------------------------';

		print '>> Truncating Table: crm_cust_info';

		SET @START_TIME = GETDATE();
		
		IF OBJECT_ID('bronze_layer.crm_cust_info','U') IS NOT NULL
			TRUNCATE TABLE bronze_layer.crm_cust_info;

		print '>> Inserting Data into: crm_cust_info';
		BULK INSERT bronze_layer.crm_cust_info
		FROM 'C:\Users\Cal\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @END_TIME = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
		PRINT '>> --------------------------'

		print '>> Truncating Table: crm_prd_info';

		SET @START_TIME = GETDATE();

		IF OBJECT_ID('bronze_layer.crm_prd_info','U') IS NOT NULL
			TRUNCATE TABLE bronze_layer.crm_prd_info;

		print '>> Inserting data Into: crm_prd_info';

		BULK INSERT bronze_layer.crm_prd_info
		FROM 'C:\Users\Cal\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @END_TIME = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
		PRINT '>> --------------------------'

		print '>> Truncating Table: crm_sales_details';

		SET @START_TIME = GETDATE();

		IF OBJECT_ID('bronze_layer.crm_sales_details','U') IS NOT NULL
			TRUNCATE TABLE bronze_layer.crm_sales_details;

		print '>> Inserting data into: crm_sales_details';

		BULK INSERT bronze_layer.crm_sales_details
		FROM 'C:\Users\Cal\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @END_TIME = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
		PRINT '>> --------------------------'

		print '------------------------------------------------';
		print 'Loading the ERP Tables';
		print '------------------------------------------------';

		print '>> Truncating Table: erp_cust_az12';

		SET @START_TIME = GETDATE();

		IF OBJECT_ID('bronze_layer.erp_cust_az12','U') IS NOT NULL
			TRUNCATE TABLE bronze_layer.erp_cust_az12;

		print '>> Inserting data into: erp_cust_az12';

		BULK INSERT bronze_layer.erp_cust_az12
		FROM 'C:\Users\Cal\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
		WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @END_TIME = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
		PRINT '>> --------------------------'

		print '>> Truncating Table: erp_loc_a101';

		IF OBJECT_ID('bronze_layer.erp_loc_a101','U') IS NOT NULL
			TRUNCATE TABLE bronze_layer.erp_loc_a101;

		print '>> Inserting data into: erp_loc_a101';

		SET @START_TIME = GETDATE();

		BULK INSERT bronze_layer.erp_loc_a101
		FROM 'C:\Users\Cal\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
		WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @END_TIME = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
		PRINT '>> --------------------------'

		print '>> Truncating Table: erp_px_cat_g1v2';

		SET @START_TIME = GETDATE();

		IF OBJECT_ID('bronze_layer.erp_px_cat_g1v2','U') IS NOT NULL
			TRUNCATE TABLE bronze_layer.erp_px_cat_g1v2;

		print '>> Inserting data into: erp_px_cat_g1v2';

		BULK INSERT bronze_layer.erp_px_cat_g1v2
		FROM 'C:\Users\Cal\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
		WITH( 
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK
		);

		SET @END_TIME = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @START_TIME,@END_TIME) AS NVARCHAR) + ' Seconds';
		PRINT '>> --------------------------'

		
		SET @BATCH_END_TIME = GETDATE();
		print '>> ========================';
		print '>> Batch process completed successfully';
		PRINT '>> Batch Load Duration: ' + CAST(DATEDIFF(second, @BATCH_START_TIME,@BATCH_END_TIME) AS NVARCHAR) + ' Seconds';
		PRINT '>> ========================'

	END TRY
	BEGIN CATCH
		print '================================================';
		print 'ERROR OCCURED IN THE BRONZE LAYER';
		print 'Error Message' + ERROR_MESSAGE();
		print 'Error Number' + CAST(ERROR_NUMBER() AS NVARCHAR);
		print 'Error State' + CAST(ERROR_STATE() AS NVARCHAR);
		print '================================================';
	END CATCH
END
