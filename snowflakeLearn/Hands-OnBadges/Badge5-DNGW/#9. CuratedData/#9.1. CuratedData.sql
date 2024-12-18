--- Curated Data
-- Turn Things Off
show tasks in account; -- suspend all tasks
alter pipe AGS_GAME_AUDIENCE.RAW.PIPE_GET_NEW_FILES set pipe_execution_paused = true;

-- Create a CURATED Layer
USE ROLE SYSADMIN;
CREATE SCHEMA AGS_GAME_AUDIENCE.CURATED;



--- Dashboards for Light Analysis
-- Create a New Dashboard and Add a Tile
-- Create a Chart



--- More Tiles = More Analysis
-- Add a Time of Day Chart



--- Aggregating Events by User
-- Rolling Up Login and Logout Events with ListAgg
select GAMER_NAME
      , listagg(GAME_EVENT_LTZ,' / ') as login_and_logout
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED 
group by gamer_name;


-- Windowed Data for Calculating Time in Game Per Player
select GAMER_NAME
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
order by game_session_length desc;

-- Code for the Heatgrid
--We added a case statement to bucket the session lengths
select case when game_session_length < 10 then '< 10 mins'
            when game_session_length < 20 then '10 to 19 mins'
            when game_session_length < 30 then '20 to 29 mins'
            when game_session_length < 40 then '30 to 39 mins'
            else '> 40 mins' 
            end as session_length
            ,tod_name
from (
select GAMER_NAME
       , tod_name
       ,game_event_ltz as login 
       ,lead(game_event_ltz) 
                OVER (
                    partition by GAMER_NAME 
                    order by GAME_EVENT_LTZ
                ) as logout
       ,coalesce(datediff('mi', login, logout),0) as game_session_length
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED_UF)
where logout is not null;

-- Add a Heatgrid for Session Length x Time of Day
