--- Productionizing the Load
-- Create a Simple Task
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 minute'
AS
    SELECT 'hello';



--- Seeing Tasks in Action
--  SYSADMIN Privileges for Executing Tasks
USE ROLE ACCOUNTADMIN;
GRANT EXECUTE TASK ON ACCOUNT TO ROLE SYSADMIN;
USE ROLE SYSADMIN;

-- Run tasks
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

-- Show tasks
SHOW TASKS IN ACCOUNT;

-- show specific task
DESCRIBE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;



--- Add Real Logic to our Task
-- Use the former CTAS Logic in the Task
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 minute'
AS 
    (
        SELECT  logs.ip_address
                , logs.user_login AS GAMER_NAME
                , logs.user_event AS GAME_EVENT_NAME
                , logs.datetime_iso8601 AS GAME_EVENT_UTC
                , loc.city
                , loc.region
                , loc.country
                , loc.timezone AS GAMER_LTZ_NAME
                , CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601) AS game_event_ltz
                , DAYNAME(game_event_ltz) AS DOW_NAME
                , lu.tod_name AS TOD_NAME 
        from    AGS_GAME_AUDIENCE.RAW.LOGS logs
        JOIN    IPINFO_GEOLOC.demo.location loc 
        ON      IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND     IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        join    ags_game_audience.raw.time_of_day_lu lu
        on      HOUR(game_event_ltz) = lu.hour
    );

--make a note of how many rows you have in the table
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added (if any!)
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;



--- Taming our Task
-- Convert Your Task so It Inserts Rows
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 minute'
AS 
    INSERT INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
        SELECT  logs.ip_address
                , logs.user_login AS GAMER_NAME
                , logs.user_event AS GAME_EVENT_NAME
                , logs.datetime_iso8601 AS GAME_EVENT_UTC
                , loc.city
                , loc.region
                , loc.country
                , loc.timezone AS GAMER_LTZ_NAME
                , CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601) AS game_event_ltz
                , DAYNAME(game_event_ltz) AS DOW_NAME
                , lu.tod_name AS TOD_NAME 
        from    AGS_GAME_AUDIENCE.RAW.LOGS logs
        JOIN    IPINFO_GEOLOC.demo.location loc 
        ON      IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND     IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        join    ags_game_audience.raw.time_of_day_lu lu
        on      HOUR(game_event_ltz) = lu.hour
    ;

--Run the task to load more rows
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added (if any!)
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;



--- Exploring Productionized Loading Methods
-- Trunc & Reload Like It's Y2K!
TRUNCATE TABLE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;
INSERT INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
    SELECT  logs.ip_address
            , logs.user_login AS GAMER_NAME
            , logs.user_event AS GAME_EVENT_NAME
            , logs.datetime_iso8601 AS GAME_EVENT_UTC
            , loc.city
            , loc.region
            , loc.country
            , loc.timezone AS GAMER_LTZ_NAME
            , CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601) AS game_event_ltz
            , DAYNAME(game_event_ltz) AS DOW_NAME
            , lu.tod_name AS TOD_NAME 
    from    AGS_GAME_AUDIENCE.RAW.LOGS logs
    JOIN    IPINFO_GEOLOC.demo.location loc 
    ON      IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
    AND     IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
    BETWEEN start_ip_int AND end_ip_int
    join    ags_game_audience.raw.time_of_day_lu lu
    on      HOUR(game_event_ltz) = lu.hour
;

-- Create a Backup Copy of the Table
CREATE TABLE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_UF
CLONE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;



--- MERGES: Build an Insert Merge
-- Build Your Insert Merge
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED E
USING (
        SELECT  logs.ip_address
            , logs.user_login AS GAMER_NAME
            , logs.user_event AS GAME_EVENT_NAME
            , logs.datetime_iso8601 AS GAME_EVENT_UTC
            , loc.city
            , loc.region
            , loc.country
            , loc.timezone AS GAMER_LTZ_NAME
            , CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601) AS game_event_ltz
            , DAYNAME(game_event_ltz) AS DOW_NAME
            , lu.tod_name AS TOD_NAME 
        from    AGS_GAME_AUDIENCE.RAW.LOGS logs
        JOIN    IPINFO_GEOLOC.demo.location loc 
        ON      IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND     IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
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



--- Test your Insert Marge 
-- Truncate Again for a Fresh Start
TRUNCATE TABLE AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED E
USING (
        SELECT  logs.ip_address
            , logs.user_login AS GAMER_NAME
            , logs.user_event AS GAME_EVENT_NAME
            , logs.datetime_iso8601 AS GAME_EVENT_UTC
            , loc.city
            , loc.region
            , loc.country
            , loc.timezone AS GAMER_LTZ_NAME
            , CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601) AS game_event_ltz
            , DAYNAME(game_event_ltz) AS DOW_NAME
            , lu.tod_name AS TOD_NAME 
        from    AGS_GAME_AUDIENCE.RAW.LOGS logs
        JOIN    IPINFO_GEOLOC.demo.location loc 
        ON      IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
        AND     IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
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



--- One Bite at a Time
-- Re-Create your task with merge statement
CREATE OR REPLACE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
    WAREHOUSE = 'COMPUTE_WH'
    SCHEDULE = '5 minute'
AS 
    MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED E
    USING (
            SELECT  logs.ip_address
                , logs.user_login AS GAMER_NAME
                , logs.user_event AS GAME_EVENT_NAME
                , logs.datetime_iso8601 AS GAME_EVENT_UTC
                , loc.city
                , loc.region
                , loc.country
                , loc.timezone AS GAMER_LTZ_NAME
                , CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601) AS game_event_ltz
                , DAYNAME(game_event_ltz) AS DOW_NAME
                , lu.tod_name AS TOD_NAME 
            from    AGS_GAME_AUDIENCE.RAW.LOGS logs
            JOIN    IPINFO_GEOLOC.demo.location loc 
            ON      IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
            AND     IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
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

-- Testing Cycle (Optional)
--Write down the number of records in your table 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the Merge a few times. No new rows should be added at this time 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if your row count changed 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Insert a test record into your Raw Table 
--You can change the user_event field each time to create "new" records 
--editing the ip_address or datetime_iso8601 can complicate things more than they need to 
--editing the user_login will make it harder to remove the fake records after you finish testing 
INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');

--After inserting a new row, run the Merge again 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--When you are confident your merge is working, you can delete the raw records 
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;



---
ALTER TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED SUSPEND;