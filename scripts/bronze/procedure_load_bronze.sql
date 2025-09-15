/*
==========================================================
Store Procedure
=========================================================

Script Purpose: 
    This stored procedure, bronze.load_bronze, automates the loading process for the bronze data layer in a data warehouse environment. 
    It performs ETL (Extract, Transform, Load) tasks by:
      >> Truncating existing tables in the bronze schema
      >> Bulk loading data from multiple CSV files into these tables
      >> Printing step-by-step status messages with timings for each operation
      >> Handling and reporting errors using structured error handling (TRY...CATCH), logging error details if any exception occurs.

Parameters: 
    There are no input parameters for this stored procedure.
    All actions are self-contained, operating on specific source files and destination tables as defined within the script. 
    Any relevant timings and information are managed via internal procedure variables.

Usage Example:
    EXEC bronze.load_bronze;
*/
CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
BEGIN
		DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
		BEGIN TRY
				SET @batch_start_time = GETDATE();
				PRINT '===================================================';
				PRINT '  >> LOADING BRONZE LAYER';
				PRINT '===================================================';

				PRINT '----------------------------------------------';
				PRINT ' >> LOADING CRM TABLES';
				PRINT '----------------------------------------------';

				SET @start_time = GETDATE();
				PRINT '>> TRUNCATING TABLE: bronze.crm_cust_info' ;
				TRUNCATE TABLE bronze.crm_cust_info;

				PRINT '>> INSERTING DATA INTO: bronze.crm_cust_info'
				BULK INSERT bronze.crm_cust_info
				FROM 'C:\Users\ASUS\OneDrive\Documents\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				)
				SET @end_time = GETDATE();
				PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '-----------------------------------------------';

				---------------------------------------------------------------------------------------------
				SET @start_time = GETDATE();
				PRINT '>> TRUNCATING TABLE: bronze.crm_prd_info;';
				TRUNCATE TABLE bronze.crm_prd_info;

				PRINT '>> INSERTING DATA INTO: bronze.crm_prd_info;'
				BULK INSERT bronze.crm_prd_info
				FROM 'C:\Users\ASUS\OneDrive\Documents\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				);
			   SET @end_time = GETDATE()
				PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '------------------------------------------------'
				------------------------------------------------------------------------------------------------
				SET @start_time = GETDATE();
				PRINT ' TRIUNCATING TABLE: bronze.crm_sales_details';
				TRUNCATE TABLE bronze.crm_sales_details;

				PRINT '>> INSERTING DATA INTO: bronze.crm_sales_details'
				BULK INSERT bronze.crm_sales_details
				FROM 'C:\Users\ASUS\OneDrive\Documents\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				);
				SET @end_time = GETDATE();
				PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '------------------------------------------------';
				-------------------------------------------------------------------------------------------------

				PRINT '----------------------------------------------';
				PRINT 'ERP SOURCE';
				PRINT '----------------------------------------------';

				SET @start_time = GETDATE();
				PRINT '>> TRUNCATING TABLE: bronze.erp_cust_az12';
				TRUNCATE TABLE bronze.erp_cust_az12;

				PRINT '>> INSERTING DATA INTO: bronze.erp_cust_az12';
				BULK INSERT bronze.erp_cust_az12
				FROM 'C:\Users\ASUS\OneDrive\Documents\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				);
				SET @end_time = GETDATE();
				PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '-----------------------------------------------';
				-------------------------------------------------------------------------------------------------
				SET @start_time = GETDATE();
				PRINT '>> TRUNCATING TABLE: bronze.erp_loc_a101';
				TRUNCATE TABLE bronze.erp_loc_a101;

				PRINT '>> INSERTING DATA INTO: bronze.erp_loc_a101';
				BULK INSERT bronze.erp_loc_a101
				FROM 'C:\Users\ASUS\OneDrive\Documents\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				);
				SET @end_time = GETDATE();
				PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '---------------------------------------------';
				---------------------------------------------------------------------------------------------------
				SET @start_time = GETDATE();
				PRINT '>> TRUNCATING TABLE: bronze.erp_px_cat_g1v2';
				TRUNCATE TABLE bronze.erp_px_cat_g1v2;

				PRINT '>> INSERTING DATA INTO: bronze.erp_px_cat_g1v2';
				BULK INSERT bronze.erp_px_cat_g1v2
				FROM 'C:\Users\ASUS\OneDrive\Documents\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
				WITH (
					FIRSTROW = 2,
					FIELDTERMINATOR = ',',
					TABLOCK
				);
				SET @end_time = GETDATE();
				PRINT '>> LOADING DURATION: '+ CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
				PRINT '----------------------------------------------'
				SET @batch_end_time = GETDATE();
				PRINT 'LOADING BRONZE LAYER IS COMPLETED'
				PRINT '>> TOTAL LOADING DURATION: '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time)AS NVARCHAR) + ' seconds'
				---------------------------------------------------------------------------------------------------
		END TRY
		BEGIN CATCH
			PRINT '========================================================';
			PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
			PRINT 'ERROR_MESSAGE: '+ ERROR_MESSAGE();
			PRINT 'ERROR_MESSAGE: '+ CAST(ERROR_NUMBER() AS NVARCHAR);
			PRINT 'ERROR_MESSAGE: '+ CAST(ERROR_STATE() AS NVARCHAR);
			PRINT '========================================================';
		END CATCH
END
