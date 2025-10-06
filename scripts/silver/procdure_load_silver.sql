/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze → Silver)
===============================================================================
Purpose:
    This stored procedure executes the ETL (Extract, Transform, Load) process 
    to populate tables in the 'silver' schema using data from the 'bronze' schema.

Operations Performed:
    - Truncates existing tables in the Silver layer.
    - Transforms and cleanses data sourced from the Bronze layer.
    - Loads the processed data into the corresponding Silver tables.

Parameters:
    None.
    This procedure does not accept any input parameters or return any output values.

Usage:
    EXEC silver.load_silver;
===============================================================================
*/


create or alter procedure silver.load_silver AS
BEGIN
	SET NOCOUNT ON
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	DECLARE @row_count INT
  BEGIN TRY
		SET @batch_start_time = GETDATE()
print '-------------------------------------------------------------------------------------------------------------------'
			SET @start_time = GETDATE()
			PRINT '>> Truncated the table'
			TRUNCATE TABLE silver.crm_cust_info
			--INSERTING DATA FROM BRONZE TO SILVER
			INSERT INTO silver.crm_cust_info(
				cst_id,
				cst_key,
				cst_firstname,
				cst_lastname,
				cst_marital_status,
				cst_gndr,
				cst_create_date
			)
			SELECT
			cst_id,
			cst_key,
			TRIM(cst_firstname) AS cst_firstname,
			TRIM(cst_lastname) AS cst_lastname,
			case when upper(TRIM(cst_marital_status)) = 'S' then 'Single'
				 when upper(TRIM(cst_marital_status)) = 'M' then 'Married'
				 else 'n/a'
			end cst_material_status,
			case when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
				 when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
				 else 'n/a'
			end cst_gndr,
			cst_create_date
			from (
			SELECT 
			* ,
			ROW_NUMBER()OVER(PARTITION BY CST_ID ORDER BY CST_CREATE_DATE DESC) AS flag_list
			FROM bronze.crm_cust_info
			)t where flag_list = 1

			SET @row_count = @@ROWCOUNT
			PRINT '>> BRONZE ---> SILVER '+CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN crm_cust_info TABLE'
			
			SET @end_time  = GETDATE()
			PRINT '>> LOAD_TIME: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds.';

print '-------------------------------------------------------------------------------------------------------------------'
			SET @start_time = GETDATE()
			PRINT '>> Truncated the table'
			TRUNCATE TABLE silver.crm_prd_info
			--INSERTING DATA FROM BRONZE TO SILVER
			INSERT INTO silver.crm_prd_info(
					prd_id,
					cat_id,
					prd_key,
					prd_nm,
					prd_cost,
					prd_line,
					prd_start_dt,
					prd_End_dt
			)
			SELECT 
				   prd_id,
				   REPLACE(SUBSTRING(PRD_KEY, 1, 5), '-', '_') AS cat_id,   --extracting the cat_id  
				   SUBSTRING(PRD_KEY, 7, LEN(PRD_KEY)) AS prd_key,          --extracting the prd_key
				   prd_nm,
				   ISNULL(prd_cost, 0) AS prd_cost,
				   CASE UPPER(TRIM(PRD_LINE))
						WHEN 'M' THEN 'Mountain'
						WHEN 'R' THEN 'Road'
						WHEN 'S' THEN 'Other Sale'
						WHEN 'T' THEN 'Touring'
						ELSE 'n/a'
				  END AS prd_line,
				   CAST(prd_start_dt AS DATE) AS prd_start_dt,
				   CAST(LEAD(prd_start_dt) OVER(PARTITION BY PRD_KEY ORDER BY prd_start_dt)-1 AS DATE) AS PRD_END_DT_TEST
			FROM bronze.crm_prd_info 

			SET @row_count = @@ROWCOUNT
			PRINT '>> BRONZE ---> SILVER '+CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN crm_prd_info TABLE'
			
			SET @end_time  = GETDATE()
			PRINT '>> LOAD_TIME: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds.';

print '-------------------------------------------------------------------------------------------------------------------'
			SET @start_time = GETDATE()
			PRINT '>> Truncated the table'
			TRUNCATE TABLE silver.crm_sales_details
			--INSERTING DATA FROM BRONZE TO SILVER
			INSERT INTO silver.crm_sales_details(
				   sls_ord_num,
				   sls_prd_key,
				   sls_cust_id,
				   sls_order_dt,
				   sls_ship_dt,
				   sls_due_dt,
				   sls_sales,
				   sls_quantity,
				   sls_price
			)
			SELECT 
				   sls_ord_num,
				   sls_prd_key,
				   sls_cust_id,
				   case 
						when sls_order_dt = 0 or LEN(sls_order_dt) != 8 THEN NULL
						else cast(cast(sls_order_dt AS VARCHAR) AS DATE)
				   END sls_order_dt,
				   CASE
						WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 Then Null
						ELSE CAST(cast(sls_ship_dt AS VARCHAR) AS DATE)
					END sls_ship_dt,
					CASE
						WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
						ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
					END sls_due_dt,
				   CASE
						WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
						THEN sls_quantity * ABS(sls_price)
						ELSE sls_sales
					END sls_sales,
				   sls_quantity,
				   CASE 
						WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity, 0)
						ELSE sls_price
					END sls_price
			  FROM bronze.crm_sales_details

			SET @row_count = @@ROWCOUNT
			PRINT '>> BRONZE ---> SILVER '+CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN crm_sales_details TABLE'
			
			SET @end_time  = GETDATE()
			PRINT '>> LOAD_TIME: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds.';

print '-------------------------------------------------------------------------------------------------------------------'
			SET @start_time = GETDATE()
			PRINT '>> Truncated the table'
			TRUNCATE TABLE silver.erp_cust_az12
			--INSERTING DATA FROM BRONZE TO SILVER
			INSERT INTO silver.erp_cust_az12(
				cid,
				bdate,
				gen
			)select 
			CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
				ELSE cid
			END as cid,
			CASE 
				WHEN bdate > GETDATE() THEN NULL
				ELSE bdate
			END bdate,
			CASE
				WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
				WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'MALE'
				ELSE 'n/a'
			END as gen 
			from bronze.erp_cust_az12

			SET @row_count = @@ROWCOUNT
			PRINT '>> BRONZE ---> SILVER '+CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN erp_cust_az12 TABLE'
			
			SET @end_time  = GETDATE()
			PRINT '>> LOAD_TIME: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds.';

print '-------------------------------------------------------------------------------------------------------------------'
			SET @start_time = GETDATE()
			PRINT '>> Truncated the table'
			TRUNCATE TABLE silver.erp_loc_a101
			--INSERTING DATA FROM BRONZE TO SILVER
			INSERT INTO silver.erp_loc_a101(
					cid,
					cntry
			)SELECT 
			REPLACE(cid, '-', '') as cid,
			CASE
				WHEN UPPER(TRIM(cntry)) = 'DE' THEN 'Germany'
				WHEN UPPER(TRIM(CNTRY)) IN ('US', 'USA') THEN 'United States'
				WHEN UPPER(TRIM(cntry)) = '' or UPPER(TRIM(cntry)) IS NULL THEN 'n/a'
				ELSE TRIM(cntry)
			END cntry
			from bronze.erp_loc_a101

			SET @row_count = @@ROWCOUNT
			PRINT '>> BRONZE ---> SILVER '+CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN erp_loc_a101 TABLE'
			
			SET @end_time  = GETDATE()
			PRINT '>> LOAD_TIME: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds.';

print '-------------------------------------------------------------------------------------------------------------------'
			SET @start_time = GETDATE()
			PRINT '>> Truncated the table'
			TRUNCATE TABLE silver.erp_px_cat_g1v2
			--INSERTING DATA FROM BRONZE TO SILVER
			INSERT INTO silver.erp_px_cat_g1v2(
			id, cat, subcat, maintenance
			)select 
			id,
			cat,
			subcat,
			maintenance
			from bronze.erp_px_cat_g1v2;

			SET @row_count = @@ROWCOUNT
			PRINT '>> BRONZE ---> SILVER '+CAST(@ROW_COUNT AS VARCHAR)+ ' ROWS TRANSFERED IN erp_px_cat_g1v2 TABLE'
			
			SET @end_time  = GETDATE()
			PRINT '>> LOAD_TIME: '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS VARCHAR) + ' seconds.';
print '-------------------------------------------------------------------------------------------------------------------'
		SET @batch_end_time = GETDATE()
		PRINT '>> BATCH_LOAD_TIME: '+ CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS VARCHAR)+' seconds';
  END TRY
  BEGIN CATCH
  END CATCH
END
