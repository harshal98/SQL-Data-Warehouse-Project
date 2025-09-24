/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/
---Check qualiy of silver layer crm_cust_info
----------------------------------------------------

--Check for Duplite and Null primary keys
select crm_cust_info.cst_id,Count(*)
from silver.crm_cust_info
group by cst_id
having Count(*)>1 or cst_id is NULL

----Check for unwanted blank spaces in firstname
Select cst_firstname from silver.crm_cust_info
where cst_firstname != trim(cst_firstname)

----Check for unwanted blank spaces in lastname
Select cst_lastname from silver.crm_cust_info
where cst_lastname != trim(cst_lastname)

--Check for Nulls in marital_status
Select distinct cst_marital_status
from silver.crm_cust_info

--Check for Nulls in gender
Select distinct cst_gndr
from silver.crm_cust_info


---Check qualiy of silver layer crm_sales_details
----------------------------------------------------

select 
sls_order_dt
from silver.crm_sales_details
where sls_order_dt <= 0 or Len(sls_order_dt) != 8 or sls_order_dt>20500101 or sls_order_dt<19000101

--validate prd_key exists for all the products in crm_prd_info
select sls_ord_num
from silver.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)


--validate sls_cust_id exists for all the products in crm_cust_info
select sls_cust_id
from silver.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)


--Check for invalid order dates
Select *
from silver.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt >sls_due_dt



--check for invalid  sales data
select
sls_sales,
sls_quantity,
sls_price
from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price or 
sls_sales<0 or sls_price<0 or sls_quantity<0
or sls_sales is null or sls_price is null or sls_quantity is null



---Check qualiy of silver layer erp_cust_az12
----------------------------------------------------


--validate all cid as present in custore table
select 
cid
from silver.erp_cust_az12 
where  cid not in (select cst_key from silver.crm_cust_info)

---check for bdate in future
select cid,
bdate 
from silver.erp_cust_az12
where bdate > getdate()

--Check for gen are valid
select distinct gen
from silver.erp_cust_az12



---Check qualiy of silver layer erp_loc_a101
-----------------------------------------------
--validate cid is mapping to cust_info table
--Expectation - no rows should return
select * from silver.erp_loc_a101
where cid  not in (select cst_key from silver.crm_cust_info)


---Data standarization and consistency
select distinct cntry
from silver.erp_loc_a101


---Check qualiy of silver layer crm_prd_info
-----------------------------------------------

----Mapp id to prodcat
select * 
from bronze.erp_px_cat_g1v2
where id not in (select dwh_prd_cat from silver.crm_prd_info)

--Check for unwanted spaces
Select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

--Data Standarization 
Select distinct maintenance from bronze.erp_px_cat_g1v2