--- 🥋 Querying the Parquet File
-- Look at the Parquet Data
SELECT
    $1:sequence_1::INT as sequence_1,
    $1:trail_name::VARCHAR as trail_name,
    $1:latitude::FLOAT as latitude,
    $1:longitude::FLOAT as longitude,
    $1:sequence_2::INT as sequence_2,
    $1:elevation::FLOAT as elevation
FROM 
    @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET/
(FILE_FORMAT => MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET)
ORDER BY
    sequence_1;



--- 🎯 Create a View with Nice Looking GeoSpatial Columns  
-- 🥋 Use a Select Statement to Fix Some Issues
SELECT
    $1:sequence_1::INT as point_id,
    $1:trail_name::VARCHAR as trail_name,
    $1:latitude::NUMBER(11,8) as lng,
    $1:longitude::NUMBER(11,8) as lat
FROM 
    @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET/
(FILE_FORMAT => MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET)
ORDER BY
    point_id;

-- 🎯 Create a View Called CHERRY_CREEK_TRAIL
CREATE OR REPLACE VIEW MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.CHERRY_CREEK_TRAIL
AS
(
    SELECT
        $1:sequence_1::INT as point_id,
        $1:trail_name::VARCHAR as trail_name,
        $1:latitude::NUMBER(11,8) as lng,
        $1:longitude::NUMBER(11,8) as lat
    FROM 
        @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_PARQUET/
    (FILE_FORMAT => MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_PARQUET)
    ORDER BY
        point_id
);



--- 🥋 Replace It With a Better View 
-- 🥋 Use || to Chain Lat and Lng Together into Coordinate Sets!
select 
    top 100 
    lng||' '||lat               as coord_pair,
    'POINT('||coord_pair||')'   as trail_point
from 
    MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.cherry_creek_trail;

create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.cherry_creek_trail as
select 
    $1:sequence_1 as point_id,
    $1:trail_name::varchar as trail_name,
    $1:latitude::number(11,8) as lng,
    $1:longitude::number(11,8) as lat,
    lng||' '||lat as coord_pair
from 
    @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.trails_parquet
(file_format => ff_parquet)
order by 
    point_id;



--- 🥋 Can We Generate a LINESTRING( )? 
-- 🥋 Let's Collapse Sets Of Coordinates into Linestrings! 
select 
    'LINESTRING('||
                listagg(coord_pair, ',') within group (order by point_id)||')' as my_linestring
from 
    MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.cherry_creek_trail
where 
    point_id <= 10
group by 
    trail_name;



--- 🎯 Can You Make The Whole Trail into a Single LINESTRING? 
select 
    'LINESTRING('||
                listagg(coord_pair, ',') within group (order by point_id)||')' as my_linestring
from 
    MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.cherry_creek_trail
where 
    point_id <= 2450
group by 
    trail_name;



--- 🥋 Explore the geoJSON Files
-- 🥋 Look at the geoJSON Data
SELECT 
    $1:features[0]:properties:Name::string as feature_name,
    $1:features[0]:geometry:coordinates::string as feature_coordinates,
    $1:features[0]:geometry::string as geometry,
    $1:features[0]:properties::string as feature_properties,
    $1:crs:properties:name::string as specs,
    $1 as whole_object
FROM 
    @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON/
(FILE_FORMAT => MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_JSON);



--- 🎯 Create a View Called DENVER_AREA_TRAILS
CREATE OR REPLACE VIEW MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
AS
(
    SELECT 
        $1:features[0]:properties:Name::string as feature_name,
        $1:features[0]:geometry:coordinates::string as feature_coordinates,
        $1:features[0]:geometry::string as geometry,
        $1:features[0]:properties::string as feature_properties,
        $1:crs:properties:name::string as specs,
        $1 as whole_object
    FROM 
        @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON/
    (FILE_FORMAT => MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_JSON)
);

--
SELECT * FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS;

