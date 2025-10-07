/*
===============================================================================
DDL Script: Create Gold Layer Views
===============================================================================
Purpose:
    This script defines and creates **Gold layer views** within the data warehouse. 
    The Gold layer represents the **final analytical layer**, designed using 
    a **Star Schema** consisting of dimension and fact views.

Description:
    - Each view aggregates, transforms, and enriches data sourced from the Silver layer.
    - The resulting datasets are **clean, standardized, and business-ready**, 
      supporting advanced analytics, BI dashboards, and reporting needs.

Usage Notes:
    - Execute this script after all Silver layer tables are successfully populated.
    - These views serve as the **primary data source** for reporting and visualization tools.
===============================================================================

*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================

IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
  
CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key, -- Surrogate key
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr -- CRM is the primary source for gender
		else coalesce(ca.gen, 'n/a')               -- Fallback to ERP data
	end AS gender,
	ca.bdate As birthdate,
	ci.cst_create_date AS create_date
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on  ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid

-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

CREATE VIEW gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
GO

-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO

