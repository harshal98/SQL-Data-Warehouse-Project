/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'DataWarehouse' after checking if it already exists. 
    If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas 
    within the database: 'bronze', 'silver', and 'gold'.
	
WARNING:
    Running this script will drop the entire 'DataWarehouse' database if it exists. 
    All data in the database will be permanently deleted. Proceed with caution 
    and ensure you have proper backups before running this script.
*/


use master

--DROP DataWarehouse DB if already exists
If Exists(select * from  sys.databases where name = 'DataWarehouse')
    BEGIN
    DROP DATABASE DataWarehouse
    END
GO

use DataWarehouse
-- Create the 'DataWarehouse' database
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse;
GO

-- Create Schemas
CREATE SCHEMA bronze;
GO

-- Create Schemas
CREATE SCHEMA silver;
GO

-- Create Schemas
CREATE SCHEMA gold;
GO