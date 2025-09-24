/*
===============================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze into Silver tables.
		
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

Create or Alter Procedure silver.load_silver_layer
as
Begin

	--insert data in silver.crm_cust_info table
	Truncate table silver.crm_cust_info 

	Insert into silver.crm_cust_info 
	(cst_id,cst_key,cst_firstname,cst_lastname,cst_marital_status,cst_gndr,cst_create_date)
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
	Print 'Loaded data  in  silver.crm_cust_info table ';
	Print '==================================================='


	-----insert data in silver.crm_prd_info table
	Truncate table silver.crm_prd_info

	Insert into silver.crm_prd_info
	(prd_id,dwh_prd_cat,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
	select 
	prd_id,
	Replace(SUBSTRING(prd_key,1,5),'-','_') as dwh_prd_cat, --extract prduct cat
	SUBSTRING(prd_key,7,LEN(prd_key)) as prd_key, --extract product key
	prd_nm,
	ISNULL(prd_cost,0) as prd_cost,
	Case Upper(Trim(prd_line))
		when 'S' then 'Other Sales'
		when 'R' then 'Road'
		when 'M' then 'Mountain'
		when 'T' then 'Touring'
		else 'N/A'
	END as prd_line,    ---Map product line codes to values
	Cast(prd_start_dt as date) as prd_start_dt, 
	Cast(Lead(prd_start_dt) over(partition by prd_key order by prd_start_dt asc)-1 as date) as prd_end_dt --calculate end date as one day after next start date
	from bronze.crm_prd_info
	Print 'Loaded data  in  silver.crm_prd_info table ';
	Print '==================================================='


	--insert data in silver.crm_sales_details table
	Truncate table silver.crm_sales_details

	Insert into silver.crm_sales_details
	(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)

	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	Case 
		when (sls_order_dt <= 0 or Len(sls_order_dt) != 8 or sls_order_dt>20500101 or sls_order_dt<19000101) 
		then Null
		else Cast(Cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
	Case 
		when (sls_ship_dt <= 0 or Len(sls_ship_dt) != 8 or sls_ship_dt>20500101 or sls_ship_dt<19000101) 
		then Null
		else Cast(Cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
	Case 
		when (sls_due_dt <= 0 or Len(sls_due_dt) != 8 or sls_due_dt>20500101 or sls_due_dt<19000101) then Null
		else Cast(Cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
	case 
		when sls_sales is null or sls_sales < 0 or sls_sales != sls_quantity * ABS(sls_price)  then sls_quantity * ABS(sls_price)
		else sls_sales 
		end as sls_sales,
	sls_quantity,
	case 
		when sls_price is null or sls_price < 0 then sls_sales/Nullif(sls_quantity,0)
		else sls_price
		end as sls_price
	from bronze.crm_sales_details
	Print 'Loaded data  in  silver.crm_sales_details table ';
	Print '==================================================='
	
	
	
	--insert data in silver.erp_cust_az12 table
	Truncate table silver.erp_cust_az12

	Insert into silver.erp_cust_az12
	(cid,bdate,gen)
	select
	Case 
		When cid like 'NAS%' then Substring(cid,4,LEN(cid))
		else cid 
		end as cid,
	Case 
		when bdate > GETDATE() then Null
		else bdate
		end as bdate,
	Case 
		When Upper(gen) in ('M','MALE') then 'Male'
		When Upper(gen) in ('F','FEMALE') then 'Female'
		Else 'n/a'
		End as gen
	from bronze.erp_cust_az12

	Print 'Loaded data  in  silver.erp_cust_az12 table ';
	Print '==================================================='
	

	--insert data in silver.erp_loc_a101 table
	Truncate table silver.erp_loc_a101

	Insert into silver.erp_loc_a101
	(cid,cntry)
	select 
	REPLACE(cid,'-','') as cid,
	Case 
		When Trim(cntry) = 'DE' then 'Germany'
		When Trim(cntry) in ('US','USA') then 'United States'
		When Trim(cntry) is null or len(Trim(cntry))= 0 then 'n/a'
		else Trim(cntry)
		end as cntry
	 from bronze.erp_loc_a101


	Print 'Loaded data  in  silver.erp_loc_a101 table ';
	Print '==================================================='



	--insert data in silver.erp_px_cat_g1v2 table
	Truncate table silver.erp_px_cat_g1v2

	Insert into silver.erp_px_cat_g1v2
	(id,cat,subcat,maintenance)
	select id,cat,subcat,maintenance 
	from bronze.erp_px_cat_g1v2

	Print 'Loaded data  in  silver.erp_px_cat_g1v2 table ';
	Print '==================================================='
End