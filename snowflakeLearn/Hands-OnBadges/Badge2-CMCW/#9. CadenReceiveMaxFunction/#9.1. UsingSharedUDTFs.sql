--- The VIN Data Infrastructure on the ACME Side
-- Create New Objects in ACME
USE ROLE SYSADMIN;
CREATE DATABASE STOCK;
CREATE SCHEMA STOCK.UNSOLD;
DROP SCHEMA STOCK.PUBLIC;

-- ACME's Lot Stock Table
create or replace table stock.unsold.lotstock
(
  vin varchar(25)
, exterior varchar(50)	
, interior varchar(50)
, manuf_name varchar(25)
, vehicle_type varchar(25)
, make_name varchar(25)
, plant_name varchar(25)
, model_year varchar(25)
, model_name varchar(25)
, desc1 varchar(25)
, desc2 varchar(25)
, desc3 varchar(25)
, desc4 varchar(25)
, desc5 varchar(25)
, engine varchar(25)
, drive_type varchar(25)
, transmission varchar(25)
, mpg varchar(25)
);

-- Create an AWS External Stage
CREATE OR REPLACE STAGE STOCK.UNSOLD.aws_s3_bucket
    URL = 's3://uni-cmcw/';

-- List files in the S3 Bucket
LIST @STOCK.UNSOLD.aws_s3_bucket;

-- Fill in the rest of the file name by looking at the files in your new stage
-- Replace the question marks with the file name (remember AWS is case sensitive)
select $1, $2, $3
from @STOCK.UNSOLD.aws_s3_bucket/Lotties_LotStock_Data.csv;

-- Create a File Format for ACME
CREATE FILE FORMAT UTIL_DB.PUBLIC.CSV_COMMA_LF_HEADER
type = 'CSV' 
field_delimiter = ',' 
record_delimiter = '\n' 
skip_header = 1 
field_optionally_enclosed_by = '"'  
trim_space = TRUE;

--
select $1 as VIN, $2 as Exterior, $3 as Interior
from @STOCK.UNSOLD.aws_s3_bucket/Lotties_LotStock_Data.csv
(file_format => UTIL_DB.PUBLIC.CSV_COMMA_LF_HEADER);



--- Load a File of 3 Columns into a Table of 18 Columns
--  Another File Format
CREATE FILE FORMAT util_db.public.CSV_COL_COUNT_DIFF 
type = 'CSV' 
field_delimiter = ',' 
record_delimiter = '\n' 
field_optionally_enclosed_by = '"'
trim_space = TRUE
error_on_column_count_mismatch = FALSE
parse_header = TRUE;

--
select $1 as VIN, $2 as Exterior, $3 as Interior
from @STOCK.UNSOLD.aws_s3_bucket/Lotties_LotStock_Data.csv
(file_format => UTIL_DB.PUBLIC.CSV_COL_COUNT_DIFF);

-- With a parsed header, Snowflake can MATCH BY COLUMN NAME during the COPY INTO
copy into stock.unsold.lotstock
from @stock.unsold.aws_s3_bucket/Lotties_LotStock_Data.csv
file_format = (format_name = util_db.public.csv_col_count_diff)
match_by_column_name='CASE_INSENSITIVE';

-- Check
select * from stock.unsold.lotstock;



--- Check the shared UDTF
-- Run the UDTF
select * 
from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN('5UXCR6C0XL9C77256'));




--- Combining the Table Data with the shared UDTF data
--A simple select from Lot Stock (choose any VIN from the LotStock table)
select * 
from stock.unsold.lotstock
where vin = '5J8YD4H86LL013641';

-- here we use ls for lotstock table and pf for parse function
-- this more complete statement lets us combine the data already in the table 
-- with the data returned from the parse function
select ls.vin, ls.exterior, ls.interior, pf.*
from
(select * 
from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN('5J8YD4H86LL013641'))
) pf
join stock.unsold.lotstock ls
where pf.vin = ls.vin;
;



--- Use a Variable Instead
-- We can use a local (session) variable to make it easier to change the VIN we are trying to enhance
set my_vin = '5J8YD4H86LL013641';

select $my_vin;
select ls.vin, pf.manuf_name, pf.vehicle_type
        , pf.make_name, pf.plant_name, pf.model_year
        , pf.desc1, pf.desc2, pf.desc3, pf.desc4, pf.desc5
        , pf.engine, pf.drive_type, pf.transmission, pf.mpg
from stock.unsold.lotstock ls
join 
    (   select 
          vin, manuf_name, vehicle_type
        , make_name, plant_name, model_year
        , desc1, desc2, desc3, desc4, desc5
        , engine, drive_type, transmission, mpg
        from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN($my_vin))
    ) pf
on pf.vin = ls.vin;



--- Don't Just Select It, Store It!
-- set m_vin to three first values of the lotstock table
set my_vin = 'SADCJ2FX3LA653693';
update stock.unsold.lotstock t
set manuf_name = s.manuf_name
, vehicle_type = s.vehicle_type
, make_name = s.make_name
, plant_name = s.plant_name
, model_year = s.model_year
, desc1 = s.desc1
, desc2 = s.desc2
, desc3 = s.desc3
, desc4 = s.desc4
, desc5 = s.desc5
, engine = s.engine
, drive_type = s.drive_type
, transmission = s.transmission
, mpg = s.mpg
from 
(
    select ls.vin, pf.manuf_name, pf.vehicle_type
        , pf.make_name, pf.plant_name, pf.model_year
        , pf.desc1, pf.desc2, pf.desc3, pf.desc4, pf.desc5
        , pf.engine, pf.drive_type, pf.transmission, pf.mpg
    from stock.unsold.lotstock ls
    join 
    (   select 
          vin, manuf_name, vehicle_type
        , make_name, plant_name, model_year
        , desc1, desc2, desc3, desc4, desc5
        , engine, drive_type, transmission, mpg
        from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN($my_vin))
    ) pf
    on pf.vin = ls.vin
) s
where t.vin = s.vin;

-- Check
select * from stock.unsold.lotstock;



--- USe a scripting block to update
-- Setting a Variable with a SQL Query
set row_count = (select count(*) 
                from stock.unsold.lotstock
                where manuf_name is null);

select $row_count;

-- Combining the Table Data with the Function Data using SQL Scripting Block
DECLARE
    update_stmt varchar(2000);
    res RESULTSET;
    cur CURSOR FOR select vin from stock.unsold.lotstock where manuf_name is null;
BEGIN
    OPEN cur;
    FOR each_row IN cur DO
        update_stmt := 'update stock.unsold.lotstock t '||
            'set manuf_name = s.manuf_name ' ||
            ', vehicle_type = s.vehicle_type ' ||
            ', make_name = s.make_name ' ||
            ', plant_name = s.plant_name ' ||
            ', model_year = s.model_year ' ||
            ', desc1 = s.desc1 ' ||
            ', desc2 = s.desc2 ' ||
            ', desc3 = s.desc3 ' ||
            ', desc4 = s.desc4 ' ||
            ', desc5 = s.desc5 ' ||
            ', engine = s.engine ' ||
            ', drive_type = s.drive_type ' ||
            ', transmission = s.transmission ' ||
            ', mpg = s.mpg ' ||
            'from ' ||
            '(       select ls.vin, pf.manuf_name, pf.vehicle_type ' ||
                    ', pf.make_name, pf.plant_name, pf.model_year ' ||
                    ', pf.desc1, pf.desc2, pf.desc3, pf.desc4, pf.desc5 ' ||
                    ', pf.engine, pf.drive_type, pf.transmission, pf.mpg ' ||
                'from stock.unsold.lotstock ls ' ||
                'join ' ||
                '(   select' || 
                '     vin, manuf_name, vehicle_type' ||
                '    , make_name, plant_name, model_year ' ||
                '    , desc1, desc2, desc3, desc4, desc5 ' ||
                '    , engine, drive_type, transmission, mpg ' ||
                '    from table(ADU_VIN.DECODE.PARSE_AND_ENHANCE_VIN(\'' ||
                  each_row.vin || '\')) ' ||
                ') pf ' ||
                'on pf.vin = ls.vin ' ||
            ') s ' ||
            'where t.vin = s.vin;';
        res := (EXECUTE IMMEDIATE :update_stmt);
    END FOR;
    CLOSE cur;   
END;

-- Check
select * from stock.unsold.lotstock;
