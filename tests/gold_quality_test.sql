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

--Validate all the customers are unique.
Select 
cst_id, count(*)
from (select 
crm_ci.cst_id,
crm_ci.cst_key,
crm_ci.cst_firstname,
crm_ci.cst_lastname,
crm_ci.cst_marital_status,
crm_ci.cst_gndr,
crm_ci.cst_create_date,
erp_ca.bdate,
erp_ca.gen
from silver.crm_cust_info as crm_ci
left join silver.erp_cust_az12 as erp_ca
on crm_ci.cst_key = erp_ca.cid) as t
group by cst_id
having count(*)>1


---Compare gender values of both tables
Select distinct cst_gndr,gen
from (select 
crm_ci.cst_id,
crm_ci.cst_key,
crm_ci.cst_firstname,
crm_ci.cst_lastname,
crm_ci.cst_marital_status,
crm_ci.cst_gndr,
crm_ci.cst_create_date,
erp_ca.bdate,
erp_ca.gen
from silver.crm_cust_info as crm_ci
left join silver.erp_cust_az12 as erp_ca
on crm_ci.cst_key = erp_ca.cid) as t



---validate there are no duplicate product key
select prd_key, count(*)
from (
select
crm_pi.prd_id,
crm_pi.dwh_prd_cat,
crm_pi.prd_key,
crm_pi.prd_nm,
crm_pi.prd_cost,
crm_pi.prd_line,
crm_pi.prd_start_dt,
erp_pc.cat,
erp_pc.subcat,
erp_pc.maintenance
from silver.crm_prd_info as crm_pi
left join silver.erp_px_cat_g1v2 as erp_pc 
on crm_pi.dwh_prd_cat = erp_pc.id
where crm_pi.prd_end_dt is Null) as t
group by prd_key
having count(*)>1


------validate all facts have related dimensions----------

select *
from gold.fact_sales
left join gold.dim_customers on fact_sales.customer_key = dim_customers.customer_key
left join gold.dim_products on fact_sales.product_key = dim_products.product_key
where dim_products.product_key is null or dim_customers.customer_key is null