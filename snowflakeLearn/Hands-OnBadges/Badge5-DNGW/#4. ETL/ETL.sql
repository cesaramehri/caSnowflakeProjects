--- Strategies for enhancing IP information
-- Use Snowflake's PARSE_IP Function
set prajina_ip_address = (SELECT DISTINCT(IP_ADDRESS) 
FROM AGS_GAME_AUDIENCE.RAW.LOGS
WHERE USER_LOGIN ilike '%prajina%');

-- check ip information
SELECT PARSE_IP($prajina_ip_address,'inet');

-- Pull Out PARSE_IP Results Fields
SELECT PARSE_IP($prajina_ip_address,'inet'):ipv4::TEXT;

-- Enhancement Infrastructure
CREATE SCHEMA AGS_GAME_AUDIENCE.ENHANCED;



--- MarketPlace IPInfo Free Sample Data
-- Ad the IPInfo geoloc



--- Joining Logs with Locations
-- Look Up Kishore & Prajina's Time Zone
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip($prajina_ip_address, 'inet'):ipv4
BETWEEN start_ip_int AND end_ip_int;

-- Look Up Everyone's Time Zone
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;



--- A Better Timezone Lookup
-- Use the IPInfo Functions (TO_JOIN_KEY and TO_INT) for a More Efficient Lookup
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, loc.city
, loc.region
, loc.country
, loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;



--- AN LTZ Column
-- Add a Local Time Zone Column to Your Select
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, loc.city
, loc.region
, loc.country
, loc.timezone
, CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601) AS game_event_ltz
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;



--- Planning for additional enhancement
-- Add A Column Called DOW_NAME
SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, loc.city
, loc.region
, loc.country
, loc.timezone
, CONVERT_TIMEZONE('UTC', loc.timezone, logs.datetime_iso8601) AS game_event_ltz
, DAYNAME(game_event_ltz) AS DOW_NAME
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;



--- Enabling a time of Day Enhancement
-- Create the Table and Fill in the Values
create table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

--insert statement to add all 24 rows to the table
insert into ags_game_audience.raw.time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');

-- Check the Table
select tod_name, listagg(hour,',') 
from ags_game_audience.raw.time_of_day_lu
group by tod_name;



--- Assigning Time of Day
-- A Join with a Function
SELECT logs.ip_address
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
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join ags_game_audience.raw.time_of_day_lu lu
on HOUR(game_event_ltz) = lu.hour;



--- Using CTAS Command
-- Convert a Select to a Table
CREATE OR REPLACE TABLE ags_game_audience.enhanced.logs_enhanced
AS 
(
SELECT logs.ip_address
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
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
join ags_game_audience.raw.time_of_day_lu lu
on HOUR(game_event_ltz) = lu.hour
);

-- Check
SELECT * FROM ags_game_audience.enhanced.logs_enhanced;
