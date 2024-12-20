--- Setup Iceberg External Volume and USer
-- 🥋 Create an External Volume
USE ROLE ACCOUNTADMIN;
CREATE OR REPLACE EXTERNAL VOLUME iceberg_external_volume
   STORAGE_LOCATIONS =
      (
         (
            NAME = 'iceberg-s3-us-west-2'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL = 's3://uni-dlkw-iceberg'
            STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::321463406630:role/dlkw_iceberg_role'
            STORAGE_AWS_EXTERNAL_ID = 'dlkw_iceberg_id'
         )
      );

-- 🥋 Check Your Volume (And Get the User Info for Us)
DESC EXTERNAL VOLUME iceberg_external_volume;



--- 🥋 Set Up Your Apache Iceberg DB and Table
-- 🥋 Create an Iceberg Database
create database my_iceberg_db
 catalog = 'SNOWFLAKE'
 external_volume = 'iceberg_external_volume';

-- 🥋 Create a Table 
set table_name = 'CCT_'||current_account();

create iceberg table identifier($table_name) (
    point_id number(10,0)
    , trail_name string
    , coord_pair string
    , distance_to_melanies decimal(20,10)
    , user_name string
)
  BASE_LOCATION = $table_name
  AS SELECT top 100
    point_id
    , trail_name
    , coord_pair
    , distance_to_melanies
    , current_user()
  FROM MELS_SMOOTHIE_CHALLENGE_DB.TRAILS.SMV_CHERRY_CREEK_TRAIL;

--
select * from identifier($table_name);



--- 📓 What's the Big Deal Here? 
--
update identifier($table_name)
set user_name = 'I am amazing!!'
where point_id = 1;


