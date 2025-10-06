/*
===============================================================================
Quality Checks: Silver Layer
===============================================================================
Purpose:
    This script executes a series of data quality checks to ensure consistency, 
    accuracy, and standardization across the 'silver' layer. The validations 
    help maintain data reliability before loading into the 'gold' layer.

Checks Included:
    - Detection of null or duplicate primary keys.
    - Identification of leading/trailing spaces in string fields.
    - Verification of data standardization and formatting consistency.
    - Validation of date integrity (invalid ranges or chronological errors).
    - Cross-field consistency checks between related columns.

Usage Notes:
    - Execute this script after loading data into the Silver layer.
    - Review and resolve any data discrepancies or anomalies identified 
      during the validation process.
===============================================================================
*/




--Checking the duplicates AND NULLS entries in the table of bronze schema
SELECT * FROM bronze.crm_prd_info
SELECT 
	PRD_ID,
	COUNT(*)
FROM bronze.crm_prd_info
GROUP BY PRD_ID
HAVING COUNT(*) > 1 AND PRD_ID IS NULL

--Checking unwanted spaces
SELECT 
PRD_NM
FROM bronze.crm_prd_info
WHERE PRD_NM != TRIM(prd_nm)

--checking for nulls and negative numbers
--Expectation:- NO Results

SELECT PRD_COST 
FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Data Normalization and standardization

SELECT  DISTINCT PRD_LINE 
FROM bronze.crm_prd_info

--CHECKING FOR INVALID DATE 

SELECT * 
FROM bronze.crm_prd_info
WHERE prd_End_dt < prd_start_dt

-------------------------------------silver layer---------------------
--Checking the duplicates AND NULLS entries in the table of bronze schema
--Expectation:- NO Results
SELECT * FROM silver.crm_prd_info
SELECT 
	PRD_ID,
	COUNT(*)
FROM silver.crm_prd_info
GROUP BY PRD_ID
HAVING COUNT(*) > 1 AND PRD_ID IS NULL

--Checking unwanted spaces
--Expectation:- NO Results
SELECT 
prd_nm
FROM silver.crm_prd_info
WHERE PRD_NM != TRIM(prd_nm)

--checking for nulls and negative numbers
--Expectation:- NO Results

SELECT PRD_COST 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

--Data Normalization and standardization

SELECT  DISTINCT PRD_LINE 
FROM silver.crm_prd_info

--CHECKING FOR INVALID DATE 
--Expectation:- NO Results
SELECT * 
FROM silver.crm_prd_info
WHERE prd_End_dt < prd_start_dt
==============================================================================================================================================================

--checking the matching records with cust and prd tables andunwanted spaces in order number
SELECT 
	   sls_ord_num,
	   sls_prd_key,
	   sls_cust_id,
	   sls_order_dt,
	   sls_ship_dt,
	   sls_due_dt,
	   sls_sales,
	   sls_quantity,
	   sls_price
  FROM silver.crm_sales_details
  where sls_ord_num != TRIM(sls_ord_num) or
  sls_prd_key not in (select prd_key from silver.crm_prd_info) or
   sls_cust_id not in (select cst_id from silver.crm_cust_info )


--Checking date sections FOR ORDER DATE

select 
NULLIF(sls_order_dt, 0) AS sls_order_dt
from bronze.crm_sales_details
where len(sls_order_dt) != 8 or 
	sls_order_dt <= 0 or 
	sls_order_dt > 20500101 or 
	sls_order_dt < 19000101

	
--Checking date sections FOR SHIPPING DATE

SELECT 
	NULLIF(sls_ship_dt, 0) AS  sls_ship_dt
FROM bronze.crm_sales_details
WHERE	sls_ship_dt <= 0 or 
		LEN(sls_ship_dt) != 8 or 
		sls_ship_dt > 20500101 or 
		sls_ship_dt < 19000101


--Checking date sections FOR due DATE

select 
	nullif(sls_due_dt, 0) as  sls_due_dt
from bronze.crm_sales_details
where sls_due_dt <= 0 or
	  LEN(sls_due_dt) != 8 or
	  sls_due_dt > 20500101 or
	  sls_due_dt < 19000101

--checking the invalid date order 
--(always remember that order date always be earlier than the shipping and due date)

select 
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt
from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt or sls_order_dt > sls_due_dt
--============================================================
--Business Rule 
-- sales  = quantity * price
-- negative, nulls, 0's are not allowed 
-----------------------------------------------------
--Rules
-- If sales is negative, zeros or null, derive it using Quantity and Price.
-- If Price is null or 0 then, calculate it using Sales / quantity.
-- if Prince is negative, convert it to a positive value.
-- If quantity is null or o then, calculate it using sales/ price.

--============================================================
--checking data consistency: between sales, quantity, price
-- sales  = quantity * price
-- values must not be negative, nulls, 0's 

SELECT DISTINCT
		sls_sales,
		sls_price,
		sls_quantity
FROM bronze.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity OR
	  sls_sales <= 0 or sls_quantity <= 0 or sls_price <=0 or
	  sls_sales Is NULL OR sls_price IS NULL OR sls_quantity IS NULL

--#do not directly take the action to the fault data, consult with the business analyst or source handler/ source system.
--# 1 Solution: Data issue will be  fixed direct in source system.
--# 2 Solution: Data issue will be fixed in data warehouse.

================================================================================================================================================================


