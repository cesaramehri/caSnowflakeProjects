-------------------------------- Setup --------------------------------
USE ROLE SYSADMIN;
USE DATABASE LIBRARY_CARD_CATALOG;
-------------------------------- Setup --------------------------------



-------------------------------- Create a Table Raw JSON Data --------------------------------
CREATE OR REPLACE TABLE LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON
(
    RAW_NESTED_BOOK VARIANT
);
-------------------------------- Create a Table Raw JSON Data --------------------------------



--------------------------------  Load the Data into the New Table, Using the File Format You Created --------------------------------
-- Check and adapt file format before copying into the table
SELECT * 
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE/json_book_author_nested
(FILE_FORMAT => LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT);

-- Copy into
COPY INTO LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE
FILES = ('json_book_author_nested.json')
FILE_FORMAT = (FORMAT_NAME = LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT);
--------------------------------  Load the Data into the New Table, Using the File Format You Created --------------------------------



-------------------------------- Query the JSON Data (returns the data in a way that makes it look like a normalized table) --------------------------------
SELECT RAW_NESTED_BOOK
FROM LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON;

SELECT RAW_NESTED_BOOK:year_published
FROM LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON;

SELECT RAW_NESTED_BOOK:authors
FROM LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON;
--------------------------------  Query the JSON Data (returns the data in a way that makes it look like a normalized table) --------------------------------



--------------------------------  Use the FLATTEN COMMAND on Nested Data --------------------------------
SELECT  VALUE:first_name AS FIRST_NAME
FROM    LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON
        ,LATERAL FLATTEN(INPUT => RAW_NESTED_BOOK:authors);

SELECT  VALUE:first_name AS FIRST_NAME
FROM    LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON
        ,TABLE(FLATTEN(RAW_NESTED_BOOK:authors));

SELECT  VALUE:first_name::VARCHAR AS FIRST_NAME,
        VALUE:last_name::VARCHAR AS LAST_NAME
FROM    LIBRARY_CARD_CATALOG.PUBLIC.NESTED_INGEST_JSON
        ,LATERAL FLATTEN(INPUT => RAW_NESTED_BOOK:authors);
--------------------------------  Use the FLATTEN COMMAND on Nested Data --------------------------------


