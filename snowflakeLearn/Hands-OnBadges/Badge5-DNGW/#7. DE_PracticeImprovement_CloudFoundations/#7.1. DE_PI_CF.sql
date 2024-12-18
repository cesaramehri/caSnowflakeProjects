--- Data Engineer Skillset Improvements
-- A New Select with Metadata and Pre-Load JSON Parsing 
SELECT 
    METADATA$FILENAME as log_file_name      -- corresponding name of the file from which the record is coming
  , METADATA$FILE_ROW_NUMBER as log_file_row_id      -- corresponding row number in the file of the record
  , current_timestamp(0) as load_ltz                 -- time when data was loaded from the stage
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
  (FILE_FORMAT => AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

-- Create a New Target Table to Match the Select  (Using CTAS, if you want to)
CREATE OR REPLACE TABLE AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS
AS
(
    SELECT 
        METADATA$FILENAME as log_file_name     -- corresponding name of the file from which the record is coming
      , METADATA$FILE_ROW_NUMBER as log_file_row_id      -- corresponding row number in the file of the record
      , current_timestamp(0) as load_ltz                 -- time when data was loaded from the stage
      , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
      , get($1,'user_event')::text as USER_EVENT
      , get($1,'user_login')::text as USER_LOGIN
      , get($1,'ip_address')::text as IP_ADDRESS    
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
      (FILE_FORMAT => AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS)
);

-- Check
SELECT * FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;



--- Next Up: The Improved COPY INTO
-- Create the New COPY INTO
TRUNCATE TABLE AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

COPY INTO AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS
FROM
(
    SELECT 
        METADATA$FILENAME as log_file_name     -- corresponding name of the file from which the record is coming
      , METADATA$FILE_ROW_NUMBER as log_file_row_id      -- corresponding row number in the file of the record
      , current_timestamp(0) as load_ltz                 -- time when data was loaded from the stage
      , $1:datetime_iso8601::timestamp_ntz as DATETIME_ISO8601
      , get($1,'user_event')::text as USER_EVENT
      , $1:user_login::text as USER_LOGIN
      , get($1,'ip_address')::text as IP_ADDRESS    
    FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
      (FILE_FORMAT => AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS)
)
FILE_FORMAT = (FORMAT_NAME = AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS);

-- Check
SELECT * FROM AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;



--- Developing Confidence as DE



--- Continuous Loading Pipelines



--- Crash course cloud computing



---  Kishore's Snowpipe Set Up (NOT YOURS)



---  Kishore's Snowpipe Set Up (Part 2)



---  Kishore's Snowpipe Set Up (Part 3)




