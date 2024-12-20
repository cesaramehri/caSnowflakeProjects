--- Get data from the marketplace



--- 🥋 Explore the Sonra Views
SELECT * FROM OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_EDUCATION;



--- 🥋 Using Variables in Snowflake Worksheets
-- Melanie's Location into a 2 Variables (mc for melanies cafe)
set mc_lng='-104.97300245114094';
set mc_lat='39.76471253574085';

--Confluence Park into a Variable (loc for location)
set loc_lng='-105.00840763333615'; 
set loc_lat='39.754141917497826';

--Test your variables to see if they work with the Makepoint function
select st_makepoint($mc_lng,$mc_lat) as melanies_cafe_point;
select st_makepoint($loc_lng,$loc_lat) as confluent_park_point;

--use the variables to calculate the distance from 
--Melanie's Cafe to Confluent Park
select st_distance(
        st_makepoint($mc_lng,$mc_lat)
        ,st_makepoint($loc_lng,$loc_lat)
        ) as mc_to_cp;

        

--- 🥋 Defining Our Own Function
--
CREATE SCHEMA MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS;
CREATE OR REPLACE FUNCTION MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.distance_to_mc(loc_lng number(38,32), loc_lat number(38,32))
  RETURNS FLOAT
  AS
  $$
    st_distance(st_makepoint('-104.97300245114094','39.76471253574085'),st_makepoint(loc_lng,loc_lat)
        )
  $$
  ;

-- 🥋 Test the New Function!
--Tivoli Center into the variables 
set tc_lng='-105.00532059763648'; 
set tc_lat='39.74548137398218';

select MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.distance_to_mc($tc_lng,$tc_lat);



--- 🥋 Analyze Melanie's Competition
-- 🥋 Create a List of Competing Juice Bars in the Area
select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%');
    
-- 🎯 Convert the List into a View
CREATE OR REPLACE VIEW MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.COMPETITION 
AS
(
select * 
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_AMENITY_SUSTENANCE
where 
    ((amenity in ('fast_food','cafe','restaurant','juice_bar'))
    and 
    (name ilike '%jamba%' or name ilike '%juice%'
     or name ilike '%superfruit%'))
 or 
    (cuisine like '%smoothie%' or cuisine like '%juice%')
);

-- 🥋 Which Competitor is Closest to Melanie's?
SELECT
 name
 ,cuisine
 , ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085')
    , coordinates
  ) AS distance_to_melanies
 ,*
FROM  MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.competition
ORDER by distance_to_melanies;

-- 🥋 Changing the Function to Accept a GEOGRAPHY Argument 
CREATE OR REPLACE FUNCTION MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.distance_to_mc(lng_and_lat GEOGRAPHY)
  RETURNS FLOAT
  AS
  $$
    st_distance(st_makepoint('-104.97300245114094','39.76471253574085'),
                lng_and_lat
        )
  $$
  ;

-- 🥋 Now We Can Use it In Our Sonra Select
SELECT
 name
 ,cuisine
 ,distance_to_mc(coordinates) AS distance_to_melanies
 ,*
FROM  MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.competition
ORDER by distance_to_melanies;



--- 📓 Why Are There Two UDFs with the Same Name?
-- 🥋 Different Options, Same Outcome!
-- Tattered Cover Bookstore McGregor Square
set tcb_lng='-104.9956203'; 
set tcb_lat='39.754874';

--this will run the first version of the UDF
select distance_to_mc($tcb_lng,$tcb_lat);

--this will run the second version of the UDF, bc it converts the coords 
--to a geography object before passing them into the function
select distance_to_mc(st_makepoint($tcb_lng,$tcb_lat));

--this will run the second version bc the Sonra Coordinates column
-- contains geography objects already
select name
, distance_to_mc(coordinates) as distance_to_melanies 
, ST_ASWKT(coordinates)
from OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP
where shop='books' 
and name like '%Tattered Cover%'
and addr_street like '%Wazee%';



--- 🎯 Create a View of Bike Shops in the Denver Data
CREATE OR REPLACE VIEW MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.DENVER_BIKE_SHOPS 
AS
(
select 
    name,
    ST_DISTANCE(
    st_makepoint('-104.97300245114094','39.76471253574085')
    , coordinates
  ) AS distance_to_melanies,
  coordinates
from 
    OPENSTREETMAP_DENVER.DENVER.V_OSM_DEN_SHOP_OUTDOORS_AND_SPORT_VEHICLES 
where shop = 'bicycle'
);

--
select * from MELS_SMOOTHIE_CHALLENGE_DB.LOCATIONS.DENVER_BIKE_SHOPS  ;
