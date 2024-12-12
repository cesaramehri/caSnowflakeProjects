--------------------------------- File 1 ---------------------------------
-- Create a Stage using the UI

-- Load the file to the stage using the UI

-- Describe Created Stage
DESC STAGE UTIL_DB.PUBLIC.MY_INTERNAL_STAGE;

-- Create a Table for Soil Types
CREATE OR REPLACE TABLE GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS_SOIL_TYPE
(
    PLANT_NAME VARCHAR(25),
    SOIL_TYPE NUMBER(1,0)
);

-- Create a File format
CREATE FILE FORMAT GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW
    TYPE = 'CSV'
    FIELD_DELIMITER = '|'
    SKIP_HEADER = 1
    ;

-- Copy data from your stage -> table
COPY INTO GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS_SOIL_TYPE
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE
FILES = ('VEG_NAME_TO_SOIL_TYPE_PIPE.txt')
FILE_FORMAT = (FORMAT_NAME = GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW);

-- Check
SELECT * 
FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS_SOIL_TYPE;
--------------------------------- File 1 ---------------------------------



--------------------------------- File 2 ---------------------------------
-- Upload the new file to your stage

-- Create another a new file format
CREATE FILE FORMAT GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW
    TYPE = 'CSV'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ;

---- Explore the Effect of File Formats On Data Interpretation
--The data in the file, with no FILE FORMAT specified
SELECT $1
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE/LU_SOIL_TYPE.tsv;

-- Same file but with one of the file formats we created earlier 
SELECT $1, $2, $3
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE/LU_SOIL_TYPE.tsv
(FILE_FORMAT => GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW);

--Same file but with the other file format we created earlier
SELECT $1, $2, $3
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE/LU_SOIL_TYPE.tsv
(FILE_FORMAT => GARDEN_PLANTS.VEGGIES.PIPECOLSEP_ONEHEADROW);

-- Create the correct file format to correct the data ingestion
CREATE FILE FORMAT GARDEN_PLANTS.VEGGIES.L9_CHALLENGE_FF
    TYPE = 'CSV'
    FIELD_DELIMITER = '\t'
    SKIP_HEADER = 1
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    ;

-- Explore the Effect of File Formats On Data Interpretation
SELECT $1, $2, $3
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE/LU_SOIL_TYPE.tsv
(FILE_FORMAT => GARDEN_PLANTS.VEGGIES.L9_CHALLENGE_FF);

-- Create a Soil Type Look Up Table
CREATE OR REPLACE TABLE GARDEN_PLANTS.VEGGIES.LU_SOIL_TYPE
(
    SOIL_TYPE_ID NUMBER,
    SOIL_TYPE VARCHAR(15),
    SOIL_DESCRIPTION VARCHAR(75)
);

-- Copy data from the stage -> table
COPY INTO GARDEN_PLANTS.VEGGIES.LU_SOIL_TYPE
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE
FILES = ('LU_SOIL_TYPE.tsv')
FILE_FORMAT = (FORMAT_NAME = GARDEN_PLANTS.VEGGIES.L9_CHALLENGE_FF);

-- Check
SELECT * 
FROM GARDEN_PLANTS.VEGGIES.LU_SOIL_TYPE;
--------------------------------- File 2 ---------------------------------



--------------------------------- File 3 ---------------------------------
-- Upload the new file to your stage

-- Create a Table
CREATE OR REPLACE TABLE GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS_PLANT_HEIGHT
(
    PLANT_NAME VARCHAR(25),
    UOM VARCHAR(1),
    LOW_END_OF_RANGE NUMBER(2,0),
    HIGH_END_OF_RANGE NUMBER(2,0)
);

-- Preview the data before copy into (select the appropriate file format)
SELECT $1, $2, $3, $4
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE/veg_plant_height.csv
(FILE_FORMAT => GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW);

-- Copy data from stage -> table
COPY INTO GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS_PLANT_HEIGHT
FROM @UTIL_DB.PUBLIC.MY_INTERNAL_STAGE
FILES = ('veg_plant_height.csv')
FILE_FORMAT = (FORMAT_NAME = GARDEN_PLANTS.VEGGIES.COMMASEP_DBLQUOT_ONEHEADROW);

-- Check
SELECT * 
FROM GARDEN_PLANTS.VEGGIES.VEGETABLE_DETAILS_PLANT_HEIGHT;
--------------------------------- File 3 ---------------------------------
