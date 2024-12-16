--- Set Up Max's Data Infrastructure 
-- Create db and schema
USE ROLE SYSADMIN;
CREATE DATABASE VIN;
CREATE SCHEMA VIN.DECODE;
DROP SCHEMA VIN.PUBLIC;

--We need a table that will allow WMIs to be decoded into Manufacturer Name, Country and Vehicle Type
CREATE TABLE vin.decode.wmi_to_manuf 
(
     wmi	    varchar(6)
    ,manuf_id	    number(6)
    ,manuf_name	    varchar(50)
    ,country	    varchar(50)
    ,vehicle_type    varchar(50)
 );
 
--We need a table that will allow you to go from Manufacturer to Make
--For example, Mercedes AG of Germany and Mercedes USA both roll up into Mercedes
--But they use different WMI Codes
CREATE TABLE vin.decode.manuf_to_make
(
     manuf_id	number(6)
    ,make_name	varchar(50)
    ,make_id	number(5)
);

--We need a table that can decode the model year
-- The year 2001 is represented by the digit 1
-- The year 2020 is represented by the letter L
CREATE TABLE vin.decode.model_year
(
     model_year_code	varchar(1)
    ,model_year_name	varchar(4)
);

--We need a table that can decode which plant at which 
--the vehicle was assembled
--You might have code "A" for Honda and code "A" for Ford
--so you need both the Make and the Plant Code to properly decode 
--the plant code
CREATE TABLE vin.decode.manuf_plants
(
     make_id	number(5)
    ,plant_code	varchar(1)
    ,plant_name	varchar(75)
 );
 
--We need to use a combination of both the Make and VDS 
--to decode many attributes including the engine, transmission, etc
CREATE TABLE vin.decode.make_model_vds
(
     make_id	  number(3)
    ,model_id	  number(6)
    ,model_name	  varchar(50)
    ,vds	  varchar(5)
    ,desc1	  varchar(25)
    ,desc2	  varchar(25)
    ,desc3	  varchar(50)
    ,desc4	  varchar(25)
    ,desc5	  varchar(25)
    ,body_style	  varchar(25)
    ,engine	  varchar(100)
    ,drive_type	  varchar(50)
    ,transmission varchar(50)
    ,mpg  	varchar(25)
);
