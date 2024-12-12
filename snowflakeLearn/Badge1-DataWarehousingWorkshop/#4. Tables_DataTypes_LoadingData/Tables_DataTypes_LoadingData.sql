-- Create a Table
CREATE OR REPLACE TABLE GARDEN_PLANTS.VEGGIES.ROOT_DEPTH
(
    ROOT_DEPTH_ID NUMBER(1),
    ROOT_DEPTH_CODE VARCHAR(1),
    ROOT_DEPTH_NAME VARCHAR(7),
    UNIT_OF_MEASURE VARCHAR(2),
    RANGE_MIN NUMBER(2),
    RANGE_MAX NUMBER(2)
);

-- Check Table Metadata
DESCRIBE TABLE GARDEN_PLANTS.VEGGIES.ROOT_DEPTH;

-- Query Table
SELECT * FROM GARDEN_PLANTS.VEGGIES.ROOT_DEPTH;

-- Insert one row
INSERT INTO GARDEN_PLANTS.VEGGIES.ROOT_DEPTH
VALUES ( 1,
         'S',
         'Shallow',
         'cm',
         30,
         45
);

-- Query your Table
SELECT * FROM GARDEN_PLANTS.VEGGIES.ROOT_DEPTH;

-- Add two more rows
INSERT INTO GARDEN_PLANTS.VEGGIES.ROOT_DEPTH
VALUES ( 2,
         'M',
         'Medium',
         'cm',
         45,
         60
),
       ( 3,
         'D',
         'Deep',
         'cm',
         60,
         90
);

-- Query your Table
SELECT * FROM GARDEN_PLANTS.VEGGIES.ROOT_DEPTH;