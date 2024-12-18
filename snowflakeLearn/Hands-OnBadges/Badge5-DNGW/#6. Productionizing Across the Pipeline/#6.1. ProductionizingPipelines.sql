--- We Have A Data Pipeline!
-- Create A New Stage
CREATE OR REPLACE STAGE AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
    URL = 's3://uni-kishore-pipeline';

-- Investigate the Stage
LIST @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

-- check files with file format
SELECT $1
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE/
(FILE_FORMAT => AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);



--- Another Method (Very Cool) for Getting Template Code
-- Create a new Raw Table
CREATE OR REPLACE TABLE AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
(
    RAW_LOG VARIANT
);

-- Copy Stage -> Table
COPY INTO AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE/
FILE_FORMAT = (FORMAT_NAME = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

-- Check
SELECT * FROM AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

-- Create a Step 2 Task to Run the COPY INTO
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES 
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '10 minute'
AS
    COPY INTO AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE/
    FILE_FORMAT = (FORMAT_NAME = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

-- Execute Task
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;

-- Check
SELECT * FROM AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;



--- Step 3: The JSON-Parsing View
--  Create a New JSON-Parsing View
create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	IP_ADDRESS,
	DATETIME_ISO8601,
	USER_EVENT,
	USER_LOGIN,
	RAW_LOG
) as
    (
        SELECT 
            RAW_LOG:ip_address::TEXT                    AS IP_ADDRESS,
            RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ     AS DATETIME_ISO8601,
            RAW_LOG:user_event::TEXT                    AS USER_EVENT,
            RAW_LOG:user_login::TEXT                    AS USER_LOGIN,
            RAW_LOG
        FROM 
            AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
        WHERE 
            RAW_LOG:agent IS NULL
    );

-- Check
SELECt * FROM AGS_GAME_AUDIENCE.RAW.PL_LOGS;

-- Modify the Step 4 MERGE Task
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 minute'
AS 
    MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED E
    USING (
            SELECT  pl_logs.ip_address
                , pl_logs.user_login AS GAMER_NAME
                , pl_logs.user_event AS GAME_EVENT_NAME
                , pl_logs.datetime_iso8601 AS GAME_EVENT_UTC
                , loc.city
                , loc.region
                , loc.country
                , loc.timezone AS GAMER_LTZ_NAME
                , CONVERT_TIMEZONE('UTC', loc.timezone, pl_logs.datetime_iso8601) AS game_event_ltz
                , DAYNAME(game_event_ltz) AS DOW_NAME
                , lu.tod_name AS TOD_NAME 
            from    AGS_GAME_AUDIENCE.RAW.PL_LOGS pl_logs
            JOIN    IPINFO_GEOLOC.demo.location loc 
            ON      IPINFO_GEOLOC.public.TO_JOIN_KEY(pl_logs.ip_address) = loc.join_key
            AND     IPINFO_GEOLOC.public.TO_INT(pl_logs.ip_address) 
            BETWEEN start_ip_int AND end_ip_int
            join    ags_game_audience.raw.time_of_day_lu lu
            on      HOUR(game_event_ltz) = lu.hour
        ) R
    ON 
        E.GAMER_NAME = R.GAMER_NAME
        AND E.GAME_EVENT_UTC = R.GAME_EVENT_UTC
        AND E.GAME_EVENT_NAME = R.GAME_EVENT_NAME
    WHEN NOT MATCHED THEN
        INSERT(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME)
        VALUES(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME);

--
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;


--- create daily resource monitor on your tasks (Daily_Shut_Down)


--- Tasks In Action
-- Truncate The Target Table
TRUNCATE TABLE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Turning on a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

--Turning OFF a task is done with a SUSPEND command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;


--- Check Task Performance



--- Tracing the results
--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS;

--Step 3 - number of rows in raw view (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay because not all IP addresses are available from the IPInfo share)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;



--- Fine Tuning our tasks
-- Grant Serverless Task Management to SYSADMIN
use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;
use role sysadmin;

-- Replace the WAREHOUSE Property in Your Tasks with (USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL')

-- Replace or Update the SCHEDULE Property, add dependency (after <task_name>)

-- Final new tasks
-- Get_new_files
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES 
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    SCHEDULE = '5 minute'
AS
    COPY INTO AGS_GAME_AUDIENCE.RAW.PL_GAME_LOGS
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE/
    FILE_FORMAT = (FORMAT_NAME = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

-- Logs enhanced
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    AFTER AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
AS 
    MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED E
    USING (
            SELECT  pl_logs.ip_address
                , pl_logs.user_login AS GAMER_NAME
                , pl_logs.user_event AS GAME_EVENT_NAME
                , pl_logs.datetime_iso8601 AS GAME_EVENT_UTC
                , loc.city
                , loc.region
                , loc.country
                , loc.timezone AS GAMER_LTZ_NAME
                , CONVERT_TIMEZONE('UTC', loc.timezone, pl_logs.datetime_iso8601) AS game_event_ltz
                , DAYNAME(game_event_ltz) AS DOW_NAME
                , lu.tod_name AS TOD_NAME 
            from    AGS_GAME_AUDIENCE.RAW.PL_LOGS pl_logs
            JOIN    IPINFO_GEOLOC.demo.location loc 
            ON      IPINFO_GEOLOC.public.TO_JOIN_KEY(pl_logs.ip_address) = loc.join_key
            AND     IPINFO_GEOLOC.public.TO_INT(pl_logs.ip_address) 
            BETWEEN start_ip_int AND end_ip_int
            join    ags_game_audience.raw.time_of_day_lu lu
            on      HOUR(game_event_ltz) = lu.hour
        ) R
    ON 
        E.GAMER_NAME = R.GAMER_NAME
        AND E.GAME_EVENT_UTC = R.GAME_EVENT_UTC
        AND E.GAME_EVENT_NAME = R.GAME_EVENT_NAME
    WHEN NOT MATCHED THEN
        INSERT(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME)
        VALUES(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ, DOW_NAME, TOD_NAME);

-- Resume the Tasks (Resume the dependant tasks first)
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;

--Turning OFF a task is done with a SUSPEND command
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

