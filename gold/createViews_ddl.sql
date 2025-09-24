/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

---------------------Create dim customer view---------------------
Create View gold.dim_customers as
select 
Row_number() over(order by crm_ci.cst_id) as customer_key,
crm_ci.cst_id as customer_id,
crm_ci.cst_key as customer_number,
crm_ci.cst_firstname as first_name,
crm_ci.cst_lastname as last_name,
erp_loc.cntry as country,
crm_ci.cst_marital_status as marital_status,
Case 
	when  crm_ci.cst_gndr = 'N\A' then Coalesce(erp_ca.gen,'n/a')
	else crm_ci.cst_gndr
	end as gender,
erp_ca.bdate as birth_date,
crm_ci.cst_create_date as created_date
from silver.crm_cust_info as crm_ci
left join silver.erp_cust_az12 as erp_ca
on crm_ci.cst_key = erp_ca.cid
left join silver.erp_loc_a101 as erp_loc
on crm_ci.cst_key = erp_loc.cid

GO

---------------------Create Dim Product view---------------------
Create view gold.dim_products as 
select
ROW_NUMBER() over(order by prd_start_dt,prd_key) as product_key,
crm_pi.prd_id as product_id,
crm_pi.prd_key as product_number,
crm_pi.prd_nm as product_name,
crm_pi.dwh_prd_cat as category_id,
erp_pc.cat as category,
erp_pc.subcat as subcategory,
erp_pc.maintenance,
crm_pi.prd_cost as cost,
crm_pi.prd_line as product_line,
crm_pi.prd_start_dt as start_date
from silver.crm_prd_info as crm_pi
left join silver.erp_px_cat_g1v2 as erp_pc 
on crm_pi.dwh_prd_cat = erp_pc.id
where crm_pi.prd_end_dt is Null

GO

---------------------Create fact Sales view---------------------
Create view gold.fact_sales
as 
Select
crm_sd.sls_ord_num as order_num,
dim_cs.customer_key as customer_key,
dim_pd.product_key,
crm_sd.sls_order_dt as order_date,
crm_sd.sls_ship_dt as shipping_date,
crm_sd.sls_due_dt as due_date,
crm_sd.sls_sales as sales,
crm_sd.sls_quantity as quanitity,
crm_sd.sls_price as price
from silver.crm_sales_details as crm_sd
left join gold.dim_customers as dim_cs on crm_sd.sls_cust_id = dim_cs.customer_id
left join gold.dim_products as dim_pd on crm_sd.sls_prd_key = dim_pd.product_number