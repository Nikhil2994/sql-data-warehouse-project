CREATE or ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME,@end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;
	BEGIN TRY
	    SET @batch_start_time = getdate();
		PRINT '================================';
		PRINT 'Loading bronze layer';
		PRINT '================================';

		PRINT '--------------------------------';
		Print 'Loading CRM tables';
		PRINT '--------------------------------';

		SET @start_time = GETDATE();
		PRINT '>>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;

		PRINT '>>> Inserting data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\nikhi\OneDrive\Desktop\Dataset-BARA\cust_info.csv'
		WITH (
		   firstrow = 2,
		   fieldterminator = ',',
		   TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> LOAD Duration:' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT'---------------------';

	    SET @start_time = GETDATE()
		PRINT '>>> Truncating Table: bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>>> Inserting data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\nikhi\OneDrive\Desktop\Dataset-BARA\prd_info.csv'
		WITH (
		   firstrow = 2,
		   fieldterminator = ',',
		   TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> LOAD Duration:' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT'---------------------';

		SET @start_time = GETDATE()
		PRINT '>>> Truncating Table: bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>>> Inserting data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\nikhi\OneDrive\Desktop\Dataset-BARA\sales_details.csv'
		WITH (
		   firstrow = 2,
		   fieldterminator = ',',
		   TABLOCK
		);
		SET @end_time = GETDATE();

		PRINT'>> LOAD Duration:' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT'---------------------';

	
		PRINT '--------------------------------';
		Print 'Loading ERP tables';
		PRINT '--------------------------------';

		SET @start_time = GETDATE()
		PRINT '>>> Truncating Table: bronze.erp_CUST_AZ12';
		TRUNCATE TABLE bronze.erp_CUST_AZ12;
		PRINT '>>> Inserting data into: bronze.erp_CUST_AZ12';
		BULK INSERT bronze.erp_CUST_AZ12
		FROM 'C:\Users\nikhi\OneDrive\Desktop\Dataset-BARA\CUST_AZ12.csv'
		WITH (
		   firstrow = 2,
		   fieldterminator = ',',
		   TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT'>> LOAD Duration:' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT'---------------------';


		SET @start_time = GETDATE()
		PRINT '>>> Truncating Table: bronze.erp_LOC_A10';
		TRUNCATE TABLE bronze.erp_LOC_A101;
		PRINT '>>> Inserting data into: bronze.erp_LOC_A101';
		BULK INSERT bronze.erp_LOC_A101
		FROM 'C:\Users\nikhi\OneDrive\Desktop\Dataset-BARA\LOC_A101.csv'
		WITH (
		   firstrow = 2,
		   fieldterminator = ',',
		   TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT'>> LOAD Duration:' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT'---------------------';

		SET @start_time = GETDATE()
		PRINT '>>> Truncating Table: bronze.erp_PX_CAT_G1V2';
		TRUNCATE TABLE bronze.erp_PX_CAT_G1V2;
		PRINT '>>> Inserting data into: bronze.erp_PX_CAT_G1V2';
		BULK INSERT bronze.erp_PX_CAT_G1V2
		FROM 'C:\Users\nikhi\OneDrive\Desktop\Dataset-BARA\PX_CAT_G1V2.csv'
		WITH (
		   firstrow = 2,
		   fieldterminator = ',',
		   TABLOCK
		);
		SET @end_time = GETDATE()
		PRINT'>> LOAD Duration:' + CAST(datediff(second,@start_time,@end_time) as NVARCHAR) + 'seconds';
		PRINT'---------------------';

		SET @batch_end_time = getdate();
		PRINT'>>Batch LOAD Duration:' + CAST(datediff(second,@batch_start_time,@batch_end_time) as NVARCHAR) + 'seconds';
		PRINT'---------------------'

	END TRY
	BEGIN CATCH
		PRINT '=======================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error Message' + CAST(ERROR_state() AS NVARCHAR);
		PRINT '=======================================================';
	END CATCH
END;
