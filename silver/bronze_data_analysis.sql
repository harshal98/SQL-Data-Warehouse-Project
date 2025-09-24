/*
===============================================================================
Analyse bronze data for Quality Checks
===============================================================================
Script Purpose:
    This is to analyse data on bronze and check the quality of all the tablse for below aspects
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.
===============================================================================
*/
-----crm_cust_info table -----
-----------------------------------------
---Final Query to load data in Silver table---
select 
cst_id,
cst_key,
TRIM(cst_firstname) as cst_firstname,
TRIM(cst_lastname) as cst_lastname,
Case When TRIM(Upper(cst_marital_status)) = 'M' then 'Married'
	When TRIM(Upper(cst_marital_status)) = 'S' then 'Single'
	Else 'N\A'
	END as cst_marital_status,
Case When TRIM(Upper(cst_gndr)) = 'M' then 'Male'
	When TRIM(Upper(cst_gndr)) = 'F' then 'Female'
	Else 'N\A'
	END as cst_gndr,
cst_create_date
from (Select *,
Row_Number() over(Partition by cst_id order by cst_create_date desc) as lastestdata
from bronze.crm_cust_info) as t
where lastestdata = 1 and cst_id is not null




--Check for Duplite and Null primary keys
select crm_cust_info.cst_id,Count(*)
from bronze.crm_cust_info
group by cst_id
having Count(*)>1 or cst_id is NULL

----Check for unwanted blank spaces in firstname
Select cst_firstname from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

----Check for unwanted blank spaces in lastname
Select cst_lastname from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)

--Check for Nulls in marital_status
Select distinct cst_marital_status
from bronze.crm_cust_info

--Check for Nulls in gender
Select distinct cst_gndr
from bronze.crm_cust_info



------------------------------------------------------
-----crm_prd_info table -----
-------------------------------------


---Check for duplicate prd_id
select prd_id,COUNT(*)
from bronze.crm_prd_info
group by prd_id
having COUNT(*)>1

--Check for blank space in prd_nm
select *
from bronze.crm_prd_info
where prd_nm != Trim(prd_nm)

--Check for invalid period
select *
from bronze.crm_prd_info
where prd_start_dt>prd_end_dt

--Check for invalid cost
select *
from bronze.crm_prd_info
where prd_cost<0 or prd_cost is null

--Check for invalid start date
select *
from bronze.crm_prd_info
where crm_prd_info.prd_start_dt is null


--Check for invalid prd_line date
select distinct prd_line 
from bronze.crm_prd_info



------------------------------------------------------
-----crm_sales_details table -----
-------------------------------------
--validate sls_order_dt
select 
sls_order_dt
from bronze.crm_sales_details
where sls_order_dt <= 0 or Len(sls_order_dt) != 8 or sls_order_dt>20500101 or sls_order_dt<19000101

--validate prd_key exists for all the products in crm_prd_info
select sls_ord_num
from bronze.crm_sales_details
where sls_prd_key not in (select prd_key from silver.crm_prd_info)


--validate sls_cust_id exists for all the products in crm_cust_info
select sls_cust_id
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info)


--Check for invalid order dates
Select *
from bronze.crm_sales_details
where sls_order_dt>sls_ship_dt or sls_order_dt >sls_due_dt



--check for invalid  sales data
select
sls_sales,
sls_quantity,
sls_price
from bronze.crm_sales_details
where sls_sales != sls_quantity * sls_price or 
sls_sales<0 or sls_price<0 or sls_quantity<0
or sls_sales is null or sls_price is null or sls_quantity is null



------------------------------------------------------
-----erp_cust_az12 table -----
-------------------------------------

--validate all cid as present in custore table
select 
cid
from bronze.erp_cust_az12 
where  cid not in (select cst_key from silver.crm_cust_info)

---check for bdate in future
select cid,
bdate 
from bronze.erp_cust_az12
where bdate > getdate()

--Check for gen are valid
select distinct gen
from bronze.erp_cust_az12



------------------------------------------------------
-----erp_loc_a101 table -----
-------------------------------------

--validate cid is mapping to cust_info table
--Expectation - no rows should return
select * from bronze.erp_loc_a101
where cid not in (select cst_key from silver.crm_cust_info)


---Data standarization and consistency
select distinct cntry
from bronze.erp_loc_a101


------------------------------------------------------
-----erp_px_cat_g1v2 table -----
-------------------------------------

----Mapp id to prodcat
select * 
from bronze.erp_px_cat_g1v2
where id not in (select dwh_prd_cat from silver.crm_prd_info)

--Check for unwanted spaces
Select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) or subcat != trim(subcat) or maintenance != trim(maintenance)

--Data Standarization 
Select distinct maintenance from bronze.erp_px_cat_g1v2

