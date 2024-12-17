--- Setting TZs in Snowflake
-- Change the Time Zone for Your Current Worksheet = Session
alter session set timezone = 'Pacific/Auckland';
select current_timestamp();

-- Check
show parameters like 'timezone';


--- Agnie Downloads an Updated Log File!
-- List files in stage
LIST @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE;

-- Explore files in stage
SELECT $1
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE/updated_feed
(FILE_FORMAT => AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

-- Pre-check
SELECT RAW_LOG FROM AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

-- Copy Stage -> Table
COPY INTO AGS_GAME_AUDIENCE.RAW.GAME_LOGS
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE/updated_feed
FILE_FORMAT = (FORMAT_NAME = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

-- Check final
SELECT 
    RAW_LOG,
    RAW_LOG:agent::TEXT                         AS AGENT,
    RAW_LOG:ip_address::TEXT                    AS IP_ADDRESS,
    RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ     AS DATETIME_ISO8601,
    RAW_LOG:user_event::TEXT                    AS USER_EVENT,
    RAW_LOG:user_login::TEXT                    AS USER_LOGIN,
FROM 
    AGS_GAME_AUDIENCE.RAW.GAME_LOGS;

-- Check View: Notice how we got all rows, but not the new column ip_address
SELECT * 
FROM AGS_GAME_AUDIENCE.RAW.LOGS;

-- Filter Out the Old Rows
SELECT * 
FROM AGS_GAME_AUDIENCE.RAW.LOGS
WHERE AGENT IS NULL;

-- second method to filter out old rows
select 
    RAW_LOG:ip_address::text as IP_ADDRESS,
    *
from ags_game_audience.raw.LOGS
where RAW_LOG:ip_address::text is not null;

-- Update Your LOGS View
CREATE OR REPLACE VIEW AGS_GAME_AUDIENCE.RAW.LOGS
AS
    (
        SELECT 
            RAW_LOG:ip_address::TEXT                    AS IP_ADDRESS,
            RAW_LOG:datetime_iso8601::TIMESTAMP_NTZ     AS DATETIME_ISO8601,
            RAW_LOG:user_event::TEXT                    AS USER_EVENT,
            RAW_LOG:user_login::TEXT                    AS USER_LOGIN,
            RAW_LOG
        FROM 
            AGS_GAME_AUDIENCE.RAW.GAME_LOGS
        WHERE 
            RAW_LOG:agent IS NULL
    );

-- Check
SELECT * 
FROM AGS_GAME_AUDIENCE.RAW.LOGS;

-- Find Prajina's Log Events in Your Table
SELECT * 
FROM AGS_GAME_AUDIENCE.RAW.LOGS
WHERE USER_LOGIN ilike '%prajina%';