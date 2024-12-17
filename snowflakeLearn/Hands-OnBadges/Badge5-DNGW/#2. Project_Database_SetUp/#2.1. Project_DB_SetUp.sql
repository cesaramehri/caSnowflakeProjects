--- Create the Project Infrastructure
-- Create DB and Schema
USE ROLE SYSADMIN;
CREATE DATABASE AGS_GAME_AUDIENCE;
CREATE SCHEMA AGS_GAME_AUDIENCE.RAW;
DROP SCHEMA AGS_GAME_AUDIENCE.PUBLIC;

-- Create a Table
CREATE OR REPLACE TABLE AGS_GAME_AUDIENCE.RAW.GAME_LOGS
(
    RAW_LOG VARIANT
);

-- Create a Stage
CREATE OR REPLACE STAGE AGS_GAME_AUDIENCE.RAW.UNI_KISHORE
    URL = 's3://uni-kishore';

-- Investigate the Stage
LIST @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE;

--  Create a File Format
CREATE OR REPLACE FILE FORMAT AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS
    TYPE = 'JSON'
    STRIP_OUTER_ARRAY = TRUE;

-- Explore the File in the Stage
SELECT $1
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE/kickoff
(FILE_FORMAT => AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

-- Copy Stage -> Table
COPY INTO AGS_GAME_AUDIENCE.RAW.GAME_LOGS
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE/kickoff
FILE_FORMAT = (FORMAT_NAME = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

-- Create a structured form of the table from its semi-structered form
SELECT 
    RAW_LOG:agent::TEXT                         AS AGENT,
    RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ     AS DATETIME_ISO8601,
    RAW_LOG:user_event::TEXT                    AS USER_EVENT,
    RAW_LOG:user_login::TEXT                    AS USER_LOGIN,
    RAW_LOG
FROM 
    AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

-- Create a View to Contain the Structured form of the table
CREATE OR REPLACE VIEW AGS_GAME_AUDIENCE.RAW.LOGS
AS
    (
        SELECT 
            RAW_LOG:agent::TEXT                         AS AGENT,
            RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ     AS DATETIME_ISO8601,
            RAW_LOG:user_event::TEXT                    AS USER_EVENT,
            RAW_LOG:user_login::TEXT                    AS USER_LOGIN,
            RAW_LOG
        FROM 
            AGS_GAME_AUDIENCE.RAW.GAME_LOGS
    );

-- Check your results
SELECT * FROM AGS_GAME_AUDIENCE.RAW.LOGS;