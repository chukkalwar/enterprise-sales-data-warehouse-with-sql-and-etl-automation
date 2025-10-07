/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs quality checks to validate the integrity, consistency, 
    and accuracy of the Gold Layer. These checks ensure:
    - Uniqueness of surrogate keys in dimension tables.
    - Referential integrity between fact and dimension tables.
    - Validation of relationships in the data model for analytical purposes.

Usage Notes:
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
select
cst_id,
count(*)
from(
	SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on  ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
)t group by cst_id
having count(*) > 1

-----------------------------------------------------------------------------------

	SELECT distinct
	ci.cst_gndr,
	ca.gen,
	case 
		when ci.cst_gndr != 'n/a' then ci.cst_gndr
		else coalesce(ca.gen, 'n/a')
	end as gender
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on  ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
--------------------------------------------------------------------------------------------

SELECT
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_marital_status,
	case 
		when ci.cst_gndr != ca.gen then ci.cst_gndr
		else coalesce(ca.gen, 'n/a')
	end new_gndr,
	ci.cst_create_date,
	ca.bdate,
	la.cntry
from silver.crm_cust_info ci
left join silver.erp_cust_az12 ca
on  ci.cst_key = ca.cid
left join silver.erp_loc_a101 la
on ci.cst_key = la.cid
where ci.cst_id is null

delete from silver.crm_cust_info
where cst_id is null
------------------------------------------------------------------------------------------

SELECT prd_key, count(*)
from(
SELECT 
	pn.prd_id,
	pn.prd_key,
	pn.prd_nm,
	pn.cat_id,
	pc.cat,
	pc.subcat,
	pc.maintenance,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt
FROM silver.crm_prd_info pn
left join silver.erp_px_cat_g1v2 pc
on pn.cat_id = pc.id
where pn.prd_end_dt is null
)sub group by prd_key
having count(*) > 1

---------------------------------------------------------------------------

--check all dimension table can successfully join to the fact table
-- foreign key integrity (dimension)

SELECT * FROM gold.fact_sales f
left join gold.dim_customers c
on f.customer_key = c.customer_key
left join gold.dim_products p
on f.product_key = p.product_key
where p.product_key is null or c.customer_key is null

--------------------------------------------------------------------------
