
-- create the transformation tables                                                                                                                                                                                                                                                                                                            
------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE PBI_USER.CLEAN_ACCOUNTS AS SELECT * FROM PBI_USER.STAGE_ACCOUNTS;
CREATE TABLE PBI_USER.CLEAN_DEALS AS SELECT * FROM PBI_USER.STAGE_DEALS;
CREATE TABLE PBI_USER.CLEAN_GEOGRAPHY AS SELECT * FROM PBI_USER.STAGE_GEOGRAPHY;
CREATE TABLE PBI_USER.CLEAN_PRODUCT_EVENTS AS SELECT * FROM PBI_USER.STAGE_PRODUCT_EVENTS;
CREATE TABLE PBI_USER.CLEAN_USERS AS SELECT * FROM PBI_USER.STAGE_USERS;

--  duplicate the data from your staging tables into your existing clean_ tables
SELECT 'INSERT INTO clean_' || REPLACE(table_name, 'STAGE_', '') || 
       ' SELECT * FROM ' || owner || '.' || table_name || ';' AS copy_script
FROM all_tables
WHERE owner = 'PBI_USER' 
  AND table_name LIKE 'STAGE_%';
  
  
  
-- insert data in transformation table                                                                                                                                                                                                                                                                                                                                                                                                                
-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
INSERT INTO PBI_USER.clean_ACCOUNTS SELECT * FROM PBI_USER.STAGE_ACCOUNTS;
INSERT INTO PBI_USER.clean_DEALS SELECT * FROM PBI_USER.STAGE_DEALS;
INSERT INTO PBI_USER.clean_GEOGRAPHY SELECT * FROM PBI_USER.STAGE_GEOGRAPHY;
INSERT INTO PBI_USER.clean_PRODUCT_EVENTS SELECT * FROM PBI_USER.STAGE_PRODUCT_EVENTS;
INSERT INTO PBI_USER.clean_USERS SELECT * FROM PBI_USER.STAGE_USERS;

COMMIT;


-- confirm raw data
SELECT * FROM pbi_user.stage_accounts;

SELECT * FROM pbi_user.stage_users;

SELECT * FROM pbi_user.stage_deals;

SELECT * FROM pbi_user.stage_product_events;

SELECT * FROM pbi_user.stage_geography;

-- confirm data duplicated in cleaning tables for transformation
SELECT * FROM pbi_user.clean_accounts;

SELECT * FROM pbi_user.clean_users;

SELECT * FROM pbi_user.clean_deals;

SELECT * FROM pbi_user.clean_product_events;

SELECT * FROM pbi_user.clean_geography;


-- Supposed to create the raw structure only not populate with data
-- Wipe the current data in clean tables
TRUNCATE TABLE PBI_USER.CLEAN_ACCOUNTS;
TRUNCATE TABLE PBI_USER.CLEAN_DEALS;
TRUNCATE TABLE PBI_USER.CLEAN_GEOGRAPHY;
TRUNCATE TABLE PBI_USER.CLEAN_PRODUCT_EVENTS;
TRUNCATE TABLE PBI_USER.CLEAN_USERS;

-- start over and drop all current tables
SELECT 'DROP TABLE PBI_USER.' || table_name || ';' 
FROM all_tables 
WHERE owner = 'PBI_USER' AND table_name LIKE 'CLEAN_%';

--'DROPTABLEPBI_USER.'||TABLE_NAME||';'                                                                                                                
-----------------------------------------------------------------------------------------------------------------------------------------------------
DROP TABLE PBI_USER.CLEAN_ACCOUNTS;
DROP TABLE PBI_USER.CLEAN_DEALS;
DROP TABLE PBI_USER.CLEAN_GEOGRAPHY;
DROP TABLE PBI_USER.CLEAN_PRODUCT_EVENTS;
DROP TABLE PBI_USER.CLEAN_USERS;

-- confirm cleaning tables for transformation don't exist
SELECT owner, table_name, last_analyzed
FROM all_tables 
WHERE owner = 'PBI_USER' 
  AND table_name LIKE 'CLEAN_%';

-- clear recycle bin
PURGE RECYCLEBIN;

-- Create a table structure only from raw source tables without bringing over any data and doing any transformations
SELECT 'CREATE TABLE PBI_USER.CLEAN_' || REPLACE(table_name, 'STAGE_', '') || 
       ' AS SELECT * FROM PBI_USER.' || table_name || ' WHERE 1=2;' AS script
FROM all_tables 
WHERE owner = 'PBI_USER' 
  AND table_name LIKE 'STAGE_%';
  
  -- script     
  -- This script is to generate the CREATE TABLE commands. 
  -- The WHERE 1=2 clause is the key—it forces Oracle to copy the column names and data types without moving a single row of data.

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
CREATE TABLE PBI_USER.CLEAN_ACCOUNTS AS SELECT * FROM PBI_USER.STAGE_ACCOUNTS WHERE 1=2;
CREATE TABLE PBI_USER.CLEAN_DEALS AS SELECT * FROM PBI_USER.STAGE_DEALS WHERE 1=2;
CREATE TABLE PBI_USER.CLEAN_GEOGRAPHY AS SELECT * FROM PBI_USER.STAGE_GEOGRAPHY WHERE 1=2;
CREATE TABLE PBI_USER.CLEAN_PRODUCT_EVENTS AS SELECT * FROM PBI_USER.STAGE_PRODUCT_EVENTS WHERE 1=2;
CREATE TABLE PBI_USER.CLEAN_USERS AS SELECT * FROM PBI_USER.STAGE_USERS WHERE 1=2;


-- Verification query to confirm the transformation tables are all there with a row count of zero.
SELECT table_name, num_rows 
FROM all_tables 
WHERE owner = 'PBI_USER' AND table_name LIKE 'CLEAN_%';



