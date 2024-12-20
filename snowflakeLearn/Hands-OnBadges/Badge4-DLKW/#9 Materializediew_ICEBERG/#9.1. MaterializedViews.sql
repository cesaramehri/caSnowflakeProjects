--- 🥋 Let's Try to Create an External Table 
-- 🥋 Create an External Stage for an External Table
CREATE OR REPLACE STAGE MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.EXTERNAL_AWS_DLKW
    URL = 's3://uni-dlkw';

-- Create external table
create or replace external table MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.T_CHERRY_CREEK_TRAIL(
	my_filename varchar(100) as (metadata$filename::varchar(100))
) 
location= @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.EXTERNAL_AWS_DLKW
auto_refresh = true
file_format = (type = parquet);

-- Check
SELECT * FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.T_CHERRY_CREEK_TRAIL;



--- 
-- 🥋 Create a Materialized View Version of Our New External Table
create secure materialized view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL(
	POINT_ID,
	TRAIL_NAME,
	LNG,
	LAT,
	COORD_PAIR,
    DISTANCE_TO_MELANIES
) as
select 
    value:sequence_1 as point_id,
    value:trail_name::varchar as trail_name,
    value:latitude::number(11,8) as lng,
    value:longitude::number(11,8) as lat,
    lng||' '||lat as coord_pair,
    locations.distance_to_mc(lng,lat) as distance_to_melanies
from 
    MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.T_CHERRY_CREEK_TRAIL;


