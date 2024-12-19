--- 📓 Reviewing Data Types
-- 🥋 Try This Fake Table to View Data Type Symbols
create or replace table util_db.public.my_data_types
(
  my_number number
, my_text varchar(10)
, my_bool boolean
, my_float float
, my_date date
, my_timestamp timestamp_tz
, my_variant variant
, my_array array
, my_object object
, my_geography geography
, my_geometry geometry
, my_vector vector(int,16)
);



--- 📓 Cloud Folders for Staging and Storing Files
-- 🎯 Create a Database for Zena's Athleisure Idea
USE ROLE SYSADMIN;
CREATE DATABASE ZENAS_ATHLEISURE_DB;
CREATE SCHEMA ZENAS_ATHLEISURE_DB.PRODUCTS;
DROP SCHEMA ZENAS_ATHLEISURE_DB.PUBLIC;

-- 🥋 Create an Internal Stage and Load the Sweatsuit Files Into It
CREATE OR REPLACE STAGE ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS
    DIRECTORY = (ENABLE = TRUE)
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE');

-- Download, unzip, and load the files to your stage

-- Check Stage
LIST @ZENAS_ATHLEISURE_DB.PRODUCTS.SWEATSUITS;



--- 🎯 Create Another Stage on Your Own 
-- 🎯 Create Another Internal Stage (CSE)
CREATE OR REPLACE STAGE ZENAS_ATHLEISURE_DB.PRODUCTS.PRODUCT_METADATA
    DIRECTORY = (ENABLE = TRUE);

-- Download, unzip, and load the files to your stage

-- Check Stage
LIST @ZENAS_ATHLEISURE_DB.PRODUCTS.PRODUCT_METADATA;



--- Snowflake Stage Object can be used to connect to and access files and data you never intend to load
--- a defined Snowflake Stage Object is most accurately thought of as a named gateway into a cloud folder where, presumably, data files are stored either short OR long term



--- 
-- 







