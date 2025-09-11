/*
=====================================================================
CREATE DATABASE AND SCHEMAS
=====================================================================

Script Purpose: 
               This script is designed to create a new database named DataWarehouse, ensuring that any existing instance of the database is removed beforehand. 
               If the database already exists, it will be dropped and recreated. 
               After creation, the script will establish three schemas to organize data effectively:

               'bronze'– for raw and unprocessed data
               'silver' – for cleansed and transformed data
               'gold' – for curated, business-ready data
WARNING:
       Executing this script will completely remove the existing DataWarehouse database, including all its objects and data. 
       This action is irreversible and will result in permanent data loss. 
       Ensure that appropriate backups are taken and confirm that it is safe to proceed before running the script.
*/

USE master;
GO

-- drop and recreate 'DataWarehouse' database.
IF EXISTS (SELECT 1 FROM sys.database WHERE name = 'DataWarehouse')  
BEGIN
     ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
END;
GO

--Create database 'DataWarehouse'
CREATE DATABASE DataWarehouse;
GO

USE DataWarehouse
GO

--Create Schemas.
CREATE SCHEMA bronze;
GO

CREATE SCHEMA silver;
GO

CREATE SCHEMA gold;
GO
