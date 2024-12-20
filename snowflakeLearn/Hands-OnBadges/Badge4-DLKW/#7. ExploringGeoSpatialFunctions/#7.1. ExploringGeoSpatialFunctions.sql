--- 🥋 Explore GeoSpatial Functions 
-- 🎯 TO_GEOGRAPHY Challenge Lab!!
select 
    'LINESTRING('||listagg(coord_pair, ',') within group (order by point_id)||')' as my_linestring,
    TO_GEOGRAPHY(my_linestring) as my_linestring_geo,
    ST_LENGTH(my_linestring_geo) AS length_of_trail
from 
    MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.cherry_creek_trail
group by 
    trail_name;



--- 🎯 Add Trail Length to Your View 
-- 🎯 Calculate the Lengths for the Other Trails
select 
    feature_name,
    --ST_LENGTH(TO_GEOGRAPHY(feature_coordinates)) AS wo_length,
    ST_LENGTH(TO_GEOGRAPHY(geometry)) AS geom_length
from 
    MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS;


-- 🎯 Change your DENVER_AREA_TRAILS view to include a Length Column!
select get_ddl('view', 'MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS');

create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
as
    SELECT 
        $1:features[0]:properties:Name::string as FEATURE_NAME,
        $1:features[0]:geometry:coordinates::string as FEATURE_COORDINATES,
        $1:features[0]:geometry::string as GEOMETRY,
        ST_LENGTH(TO_GEOGRAPHY(geometry)) AS TRAIL_LENGTH,
        $1:features[0]:properties::string as FEATURE_PROPERTIES,
        $1:crs:properties:name::string as SPECS,
        $1 as WHOLE_OBJECT
    FROM 
        @MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.TRAILS_GEOJSON/
    (FILE_FORMAT => MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.FF_JSON)
;

SELECT * FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS;



--- 🥋 Make the Trail Data Align to Work Together 
-- 🥋 Create a View on Cherry Creek Data to Mimic the Other Trail Data
create or replace view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS_2 as
select 
    trail_name as feature_name,
    '{"coordinates":['||listagg('['||lng||','||lat||']',',') within group (order by point_id)||'],"type":"LineString"}' as geometry
    ,st_length(to_geography(geometry))  as trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.cherry_creek_trail
group by trail_name;

SELECT * FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS_2;

-- 🥋 Use A Union All to Bring the Rows Into a Single Result Set
select 
    feature_name, 
    geometry, 
    trail_length
from 
    MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
union all
select 
    feature_name, 
    geometry, 
    trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS_2;



--- 🥋 Enhancing the GeoSpatial Trail View 
--
create view MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.trails_and_boundaries
as
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS
union all
select feature_name
, to_geography(geometry) as my_linestring
, st_xmin(my_linestring) as min_eastwest
, st_xmax(my_linestring) as max_eastwest
, st_ymin(my_linestring) as min_northsouth
, st_ymax(my_linestring) as max_northsouth
, trail_length
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.DENVER_AREA_TRAILS_2;

--
select * from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.trails_and_boundaries;



--- 🥋 Creating A Bounding Box Polygon
-- 📓  A Polygon Can be Used to Create a Bounding Box
select 'POLYGON(('|| 
    min(min_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||max(max_northsouth)||','|| 
    max(max_eastwest)||' '||min(min_northsouth)||','|| 
    min(min_eastwest)||' '||min(min_northsouth)||'))' AS my_polygon
from MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.trails_and_boundaries;


