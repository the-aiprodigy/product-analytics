-- Verification query to confirm the transformation tables are all there with a row count of zero.
SELECT table_name, num_rows 
FROM all_tables 
WHERE owner = 'PBI_USER' AND table_name LIKE 'CLEAN_%';


-- 1. Drop existing clean tables if they exist
BEGIN
   FOR r IN (SELECT table_name FROM all_tables WHERE owner = 'PBI_USER' AND table_name LIKE 'CLEAN_%') LOOP
      EXECUTE IMMEDIATE 'DROP TABLE PBI_USER.' || r.table_name;
   END LOOP;
END;


-- 2. Recreate the tables and pull in all data from STAGE_
CREATE TABLE PBI_USER.CLEAN_ACCOUNTS AS SELECT * FROM PBI_USER.STAGE_ACCOUNTS;
CREATE TABLE PBI_USER.CLEAN_DEALS AS SELECT * FROM PBI_USER.STAGE_DEALS;
CREATE TABLE PBI_USER.CLEAN_GEOGRAPHY AS SELECT * FROM PBI_USER.STAGE_GEOGRAPHY;
CREATE TABLE PBI_USER.CLEAN_PRODUCT_EVENTS AS SELECT * FROM PBI_USER.STAGE_PRODUCT_EVENTS;
CREATE TABLE PBI_USER.CLEAN_USERS AS SELECT * FROM PBI_USER.STAGE_USERS;

/*
SOURCE_     ? raw (current: stage_)
STAGE_      ? ingestion layer (current: clean_)
TRANSFORM_  ? (you’ll build next)
ANALYTICS_  ? (already designed)

Oracle does not support bulk rename with pattern, so we’ll:
Rename tables individually using RENAME
Or recreate using CREATE TABLE AS SELECT (safer for pipelines)
*/

-- ALTER TABLE: Allows you to specify the owner (pbi_user.) so Oracle knows exactly which table to find.
-- RENAME TO: Does not require the owner on the new name, as it stays in the same schema.
--1. Rename current stage_ ? source_
ALTER TABLE pbi_user.stage_accounts RENAME TO source_accounts;
ALTER TABLE pbi_user.stage_users RENAME TO source_users;
ALTER TABLE pbi_user.stage_deals RENAME TO source_deals;
ALTER TABLE pbi_user.stage_product_events RENAME TO source_product_events;
ALTER TABLE pbi_user.stage_geography RENAME TO source_geography;

--2. Rename current clean_ ? stage_
ALTER TABLE pbi_user.clean_accounts RENAME TO stage_accounts;
ALTER TABLE pbi_user.clean_users RENAME TO stage_users;
ALTER TABLE pbi_user.clean_deals RENAME TO stage_deals;
ALTER TABLE pbi_user.clean_product_events RENAME TO stage_product_events;
ALTER TABLE pbi_user.clean_geography RENAME TO stage_geography;

-- Verify the row counts and data is correct:
SELECT table_name, num_rows 
FROM all_tables 
WHERE owner = 'PBI_USER' AND table_name LIKE 'STAGE_%';

-- Verify the row counts and data is correct:
SELECT table_name, num_rows 
FROM all_tables 
WHERE owner = 'PBI_USER' AND table_name LIKE 'SOURCE_%';

/*
The (null) value in the NUM_ROWS column after trying to verify the row counts in the SOURCE_ tables doesn't necessarily mean the tables are empty; 
it just means Oracle's metadata (statistics) hasn't been updated since you renamed or created them.
In Oracle, the ALL_TABLES view shows a "snapshot" of row counts from the last time statistics were gathered. 
Since these are "new" names, the counter hasn't run yet.
*/
-- Run this command to force Oracle to count the rows and update the metadata for the PBI_USER schema:
EXEC DBMS_STATS.GATHER_SCHEMA_STATS('PBI_USER');
