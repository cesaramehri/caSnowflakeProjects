-------------------------------- Setup --------------------------------
USE ROLE SYSADMIN;
USE DATABASE LIBRARY_CARD_CATALOG;
-------------------------------- Setup --------------------------------



-------------------------------- Create a Table Raw JSON Data --------------------------------
CREATE TABLE LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON
(
    RAW_AUTHOR VARIANT
);
-------------------------------- Create a Table Raw JSON Data --------------------------------



-------------------------------- Create a File Format to Load the JSON Data --------------------------------
CREATE OR REPLACE FILE FORMAT LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    ENABLE_OCTAL = FALSE
    ALLOW_DUPLICATE = FALSE 
    STRIP_OUTER_ARRAY = TRUE
    STRIP_NULL_VALUES = FALSE 
    IGNORE_UTF8_ERRORS = FALSE; 
-------------------------------- Create a File Format to Load the JSON Data --------------------------------



--------------------------------  Load the Data into the New Table, Using the File Format You Created --------------------------------
-- Check and adapt file format before copying into the table
SELECT *
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE/author_with_header.json
(FILE_FORMAT => LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT);

-- Copy into
COPY INTO LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE
FILES = ('author_with_header.json')
FILE_FORMAT = (FORMAT_NAME = LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT);

-- Check Table
SELECT * 
FROM LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON;
--------------------------------  Load the Data into the New Table, Using the File Format You Created --------------------------------



--------------------------------  Query the JSON Data (returns the data in a way that makes it look like a normalized table) --------------------------------
SELECT  RAW_AUTHOR:AUTHOR_UID AS AUTHOR_UID,
        RAW_AUTHOR:FIRST_NAME AS FIRST_NAME,
        RAW_AUTHOR:MIDDLE_NAME AS MIDDLE_NAME,
        RAW_AUTHOR:LAST_NAME AS LAST_NAME
FROM    LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON;

-- Cast
SELECT  RAW_AUTHOR:AUTHOR_UID::NUMBER AS AUTHOR_UID,
        RAW_AUTHOR:FIRST_NAME::STRING AS FIRST_NAME,
        RAW_AUTHOR:MIDDLE_NAME::STRING AS MIDDLE_NAME,
        RAW_AUTHOR:LAST_NAME::STRING AS LAST_NAME
FROM    LIBRARY_CARD_CATALOG.PUBLIC.AUTHOR_INGEST_JSON;
--------------------------------  Query the JSON Data (returns the data in a way that makes it look like a normalized table) --------------------------------
