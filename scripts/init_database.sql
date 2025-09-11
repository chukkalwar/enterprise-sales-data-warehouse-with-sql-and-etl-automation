=====================================================================
CREATE DATABASE AND SCHEMAS
=====================================================================

Script Purpose: 
               The script creates new database named 'DataWarehouse' after checking if it already exists.
               If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas within the database:
               'bronze', 'silver', 'gold'.
WARNING:
        Running this script will drop  the enitre 'Datawarehouse' database if it exists.
        All data in the database will be permanently deleted. Proceed with caution and ensure you have proper backups  before running this scripts.

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
