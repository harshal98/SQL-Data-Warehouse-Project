/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

Create or ALter Procedure bronze.load_bronzelayer as

BEGIN
Declare @batchstarttime datetime = GETDATE() 

Print '==============================================';
Print 'Loading Data to Bronze Layer';
Print '==============================================';


Print '==============================================';
Print 'Loading CRM data to Bronze Layer';
Print '==============================================';

Declare @starttime as datetime,@endtime as datetime;
SET @starttime = GETDATE();

TRUNCATE TABLE bronze.crm_cust_info
BULK INSERT bronze.crm_cust_info
FROM 'C:\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @endtime = GETDATE();
Print 'Loaded table ' + 'crm_cust_info in ' + cast(Datediff(millisecond,@starttime,@endtime) as varchar) + ' ms'
Print '==============================================';


SET @starttime = GETDATE();

TRUNCATE TABLE bronze.crm_prd_info
BULK INSERT bronze.crm_prd_info
FROM 'C:\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @endtime = GETDATE();
Print 'Loaded table ' + 'crm_prd_info ' + cast(Datediff(millisecond,@starttime,@endtime) as varchar) + ' ms'
Print '==============================================';


SET @starttime = GETDATE();

TRUNCATE TABLE bronze.crm_sales_details
BULK INSERT bronze.crm_sales_details
FROM 'C:\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @endtime = GETDATE();
Print 'Loaded table ' + 'crm_sales_details ' + cast(Datediff(millisecond,@starttime,@endtime) as varchar) + ' ms'
Print '==============================================';



Print '==============================================';
Print 'Loading ERP Data to Bronze Layer';
Print '==============================================';

SET @starttime = GETDATE();

TRUNCATE TABLE bronze.erp_cust_az12
BULK INSERT bronze.erp_cust_az12
FROM 'C:\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @endtime = GETDATE();
Print 'Loaded table ' + 'erp_cust_az12 ' + cast(Datediff(millisecond,@starttime,@endtime) as varchar) + ' ms'
Print '==============================================';

SET @starttime = GETDATE();

TRUNCATE TABLE bronze.erp_loc_a101
BULK INSERT bronze.erp_loc_a101
FROM 'C:\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);


SET @endtime = GETDATE();
Print 'Loaded table ' + 'erp_loc_a101 ' + cast(Datediff(millisecond,@starttime,@endtime) as varchar) + ' ms'
Print '==============================================';

SET @starttime = GETDATE();

TRUNCATE TABLE bronze.erp_px_cat_g1v2
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
WITH (
	FIRSTROW = 2,
	FIELDTERMINATOR = ',',
	TABLOCK
);

SET @endtime = GETDATE();
Print 'Loaded table ' + 'erp_px_cat_g1v2 ' + cast(Datediff(millisecond,@starttime,@endtime) as varchar) + ' ms'
Print '==============================================';

Declare @batchendtime datetime = GetDate()

Print 'Batch Loaded in ' + Cast (DateDiff(millisecond,@batchstarttime,@batchendtime) as varchar) + ' ms'

END