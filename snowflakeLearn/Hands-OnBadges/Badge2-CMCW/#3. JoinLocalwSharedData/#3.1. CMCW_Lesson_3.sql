--- Set Up a New Database Called INTL_DB
USE ROLE SYSADMIN;
CREATE DATABASE INTL_DB;
USE SCHEMA INTL_DB.PUBLIC;



--- Create a Warehouse for Loading INTL_DB
USE ROLE SYSADMIN;
CREATE WAREHOUSE INTL_WH
WITH
    WAREHOUSE_SIZE = 'XSMALL'
    WAREHOUSE_TYPe = 'STANDARD'
    AUTO_SUSPEND = 600
    AUTO_RESUME = TRUE;
USE WAREHOUSE INTL_WH;



--- Create Table INT_STDS_ORG_3166
CREATE OR REPLACE TABLE INTL_DB.PUBLIC.INT_STDS_ORG_3166 
(   ISO_COUNTRY_NAME        VARCHAR(100), 
    COUNTRY_NAME_OFFICIAL   VARCHAR(200), 
    SOVREIGNTY              VARCHAR(40), 
    ALPHA_CODE_2DIGIT       VARCHAR(2), 
    ALPHA_CODE_3DIGIT       VARCHAR(3), 
    NUMERIC_COUNTRY_CODE    INTEGER,
    ISO_SUBDIVISION         VARCHAR(15), 
    INTERNET_DOMAIN_CODE    VARCHAR(10)
);



---  Create a File Format to Load the Table
CREATE OR REPLACE FILE FORMAT UTIL_DB.PUBLIC.PIPE_DBLQUOTE_HEADER_CR 
  TYPE = 'CSV'
  COMPRESSION = 'AUTO' 
  FIELD_DELIMITER = '|'
  RECORD_DELIMITER = '\r' --CARRIAGE RETURN
  SKIP_HEADER = 1
  FIELD_OPTIONALLY_ENCLOSED_BY = '\042'  --DOUBLE QUOTES
  TRIM_SPACE = FALSE;


  
--- Load the ISO Table Using Your File Format
-- Create a stage to an S3 Bucket
CREATE STAGE UTIL_DB.PUBLIC.AWS_S3_BUCKET
    URL = 's3://uni-cmcw';

-- List files in the S3 Bucket
LIST @UTIL_DB.PUBLIC.AWS_S3_BUCKET;

-- check
SELECT 
    $1, $2
FROM 
    @UTIL_DB.PUBLIC.AWS_S3_BUCKET/ISO_Countries_UTF8_pipe.csv
(FILE_FORMAT => UTIL_DB.PUBLIC.PIPE_DBLQUOTE_HEADER_CR);

-- Copy data from stage -> table
COPY INTO 
    INTL_DB.PUBLIC.INT_STDS_ORG_3166
FROM 
    @UTIL_DB.PUBLIC.AWS_S3_BUCKET
FILES = ('ISO_Countries_UTF8_pipe.csv')
FILE_FORMAT = (FORMAT_NAME = UTIL_DB.PUBLIC.PIPE_DBLQUOTE_HEADER_CR);

-- Check
SELECT 
    COUNT(*) AS FOUND, 
    '249' AS EXPECTED 
FROM 
    INTL_DB.PUBLIC.INT_STDS_ORG_3166; 

    

---- Join Local Data with Shared Data
SELECT 
    I.ISO_COUNTRY_NAME,
    I.COUNTRY_NAME_OFFICIAL,
    I.ALPHA_CODE_2DIGIT,
    R.R_NAME AS REGION
FROM 
    INTL_DB.PUBLIC.INT_STDS_ORG_3166 I
LEFT JOIN
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION N
ON
    UPPER(I.ISO_COUNTRY_NAME) = N.N_NAME
LEFT JOIN
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION R
ON
    N.N_REGIONKEY = R.R_REGIONKEY;
    

    
--- Convert the Select Statement into a View
CREATE VIEW INTL_DB.PUBLIC.NATIONS_SAMPLE_PLUS_ISO 
(
    ISO_COUNTRY_NAME,
    COUNTRY_NAME_OFFICIAL,
    ALPHA_CODE_2DIGIT,
    REGION
)
AS
SELECT 
    I.ISO_COUNTRY_NAME,
    I.COUNTRY_NAME_OFFICIAL,
    I.ALPHA_CODE_2DIGIT,
    R.R_NAME AS REGION
FROM 
    INTL_DB.PUBLIC.INT_STDS_ORG_3166 I
LEFT JOIN
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION N
ON
    UPPER(I.ISO_COUNTRY_NAME) = N.N_NAME
LEFT JOIN
    SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION R
ON
    N.N_REGIONKEY = R.R_REGIONKEY;

SELECT
    *
FROM INTL_DB.PUBLIC.NATIONS_SAMPLE_PLUS_ISO;



--- Create a Few More Tables and Load Them 
-- Create Table Currencies
CREATE TABLE INTL_DB.PUBLIC.CURRENCIES 
(
    CURRENCY_ID             INTEGER, 
    CURRENCY_CHAR_CODE      VARCHAR(3), 
    CURRENCY_SYMBOL         VARCHAR(4), 
    CURRENCY_DIGITAL_CODE   VARCHAR(3), 
    CURRENCY_DIGITAL_NAME   VARCHAR(30)
)
    comment = 'Information about currencies including character codes, symbols, digital codes, etc.';

-- Create Table Country to Currency
CREATE TABLE INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE 
(
    COUNTRY_CHAR_CODE       VARCHAR(3), 
    COUNTRY_NUMERIC_CODE    INTEGER, 
    COUNTRY_NAME            VARCHAR(100), 
    CURRENCY_NAME           VARCHAR(100), 
    CURRENCY_CHAR_CODE      VARCHAR(3), 
    CURRENCY_NUMERIC_CODE   INTEGER
) 
comment = 'Mapping table currencies to countries';

-- Create a File Format to Process files with Commas, Linefeeds and a Header Row
CREATE FILE FORMAT UTIL_DB.PUBLIC.CSV_COMMA_LF_HEADER
    TYPE = 'CSV' 
    FIELD_DELIMITER = ',' 
    RECORD_DELIMITER = '\n' -- line feed character
    SKIP_HEADER = 1;

-- Copy data from stage -> tables
COPY INTO 
    INTL_DB.PUBLIC.CURRENCIES
FROM 
    @UTIL_DB.PUBLIC.AWS_S3_BUCKET
FILES = ('currencies.csv')
FILE_FORMAT = (FORMAT_NAME = UTIL_DB.PUBLIC.CSV_COMMA_LF_HEADER);

COPY INTO 
    INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE
FROM 
    @UTIL_DB.PUBLIC.AWS_S3_BUCKET
FILES = ('country_code_to_currency_code.csv')
FILE_FORMAT = (FORMAT_NAME = UTIL_DB.PUBLIC.CSV_COMMA_LF_HEADER);



--- Create a View
CREATE VIEW INTL_DB.PUBLIC.SIMPLE_CURRENCY 
(
    CTY_CODE,
    CUR_CODE
)
AS
SELECT 
    COUNTRY_CHAR_CODE,
    CURRENCY_CHAR_CODE
FROM
    INTL_DB.PUBLIC.COUNTRY_CODE_TO_CURRENCY_CODE;


    