-------------------------------- Setup --------------------------------
USE ROLE SYSADMIN;
CREATE DATABASE SOCIAL_MEDIA_FLOODGATES;
-------------------------------- Setup --------------------------------



-------------------------------- Create a Table Raw JSON Data --------------------------------
CREATE OR REPLACE TABLE SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
(
    RAW_STATUS VARIANT
);
-------------------------------- Create a Table Raw JSON Data --------------------------------



--------------------------------  Load the Data into the New Table, Using the File Format You Created --------------------------------
-- Check and adapt file format before copying into the table
SELECT * 
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE/nutrition_tweets.json
(FILE_FORMAT => LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT);

-- Copy into
COPY INTO SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE
FILES = ('nutrition_tweets.json')
FILE_FORMAT = (FORMAT_NAME = LIBRARY_CARD_CATALOG.PUBLIC.JSON_FILE_FORMAT);
--------------------------------  Load the Data into the New Table, Using the File Format You Created --------------------------------



--------------------------------  Query the JSON Data (returns the data in a way that makes it look like a normalized table) --------------------------------
SELECT  RAW_STATUS
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;

SELECT  RAW_STATUS:entities
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;

SELECT  RAW_STATUS:entities:hashtags
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;

SELECT  RAW_STATUS:entities:hashtags[0]
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;

SELECT  RAW_STATUS:entities:hashtags[0].text
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
WHERE   RAW_STATUS:entities:hashtags[0].text IS NOT NULL;

SELECT  RAW_STATUS:created_at
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;

SELECT  RAW_STATUS:created_at::DATE
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST;
--------------------------------  Query the JSON Data (returns the data in a way that makes it look like a normalized table) --------------------------------



--------------------------------  Use the FLATTEN COMMAND on Nested Data --------------------------------
-- Use these example flatten commands to explore flattening the nested book and author data
SELECT  VALUE
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
        , LATERAL FLATTEN(INPUT => RAW_STATUS:entities:urls);

SELECT  VALUE
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
        , TABLE(FLATTEN(RAW_STATUS:entities:urls));

--------------------------------  Use the FLATTEN COMMAND on Nested Data --------------------------------



--------------------------------  Query the Nested JSON Tweet Data!  --------------------------------
SELECT  VALUE:"text"::VARCHAR AS HASHTAG_USED
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
        ,TABLE(FLATTEN(RAW_STATUS:entities:hashtags));

SELECT  RAW_STATUS:"user":name::TEXT AS USER_NAME,
        RAW_STATUS:id AS TWEET_ID,
        VALUE:"text"::VARCHAR AS HASHTAG_USED
FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
        ,TABLE(FLATTEN(RAW_STATUS:entities:hashtags));

--------------------------------  Query the Nested JSON Tweet Data!  --------------------------------



--------------------------------  Create a View of the URL Data Looking "Normalized"  --------------------------------
CREATE OR REPLACE VIEW SOCIAL_MEDIA_FLOODGATES.PUBLIC.URLS_NORMALIZED
AS
(
    SELECT  RAW_STATUS:"user":name::TEXT AS USER_NAME,
            RAW_STATUS:id AS TWEET_ID,
            VALUE:display_url::TEXT AS URL_USED
    FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
            ,TABLE(FLATTEN(RAW_STATUS:entities:urls))
);

SELECT * 
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.URLS_NORMALIZED;

CREATE OR REPLACE VIEW SOCIAL_MEDIA_FLOODGATES.PUBLIC.HASHTAGS_NORMALIZED
AS
(
    SELECT  RAW_STATUS:"user":name::TEXT AS USER_NAME,
            RAW_STATUS:id AS TWEET_ID,
            VALUE:"text"::VARCHAR AS HASHTAG_USED
    FROM    SOCIAL_MEDIA_FLOODGATES.PUBLIC.TWEET_INGEST
            ,TABLE(FLATTEN(RAW_STATUS:entities:hashtags))
);

SELECT * 
FROM SOCIAL_MEDIA_FLOODGATES.PUBLIC.HASHTAGS_NORMALIZED;

--------------------------------  Create a View of the URL Data Looking "Normalized"  --------------------------------